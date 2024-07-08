#![feature(let_chains, slice_partition_dedup, string_remove_matches, sync_unsafe_cell)]
#![allow(unused_must_use)]

mod utils;
mod converter;

use std::{
	str::FromStr,
	time::Duration,
	path::{Path, PathBuf},
	fs::File,
	collections::{HashMap, HashSet}, 
	sync::{atomic::{AtomicU64, Ordering}, Arc}, 
};

use chrono::Local;
use converter::merge_nodes;
use geo::{Centroid, Contains, Geometry, HasDimensions};
use indicatif::ProgressBar;
use osmpbfreader::{Node, OsmObj, OsmPbfReader, Relation, Tags, Way};
use parking_lot::{RwLock, Mutex};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator};
// use serde::Serialize;

trait Url {
	fn url(&self) -> String;
	fn str_id(&self) -> String;
}

impl<T: Into<OsmObj> + Clone> Url for T where OsmObj: From<T> {
	fn url(&self) -> String {
		const BASE_URL: &str = "https://www.openstreetmap.org";
		let obj = OsmObj::from(self.clone());
		let s = match obj {
			OsmObj::Way(_) => "way",
			OsmObj::Relation(_) => "relation",
			OsmObj::Node(_) => "node",
		};

		format!("{BASE_URL}/{}/{}", s, obj.id().inner_id())
	}
	
	fn str_id(&self) -> String {
		let obj = OsmObj::from(self.clone());
		let s = match obj {
			OsmObj::Way(_) => "way",
			OsmObj::Relation(_) => "relation",
			OsmObj::Node(_) => "node",
		};
		format!("{}/{}/{}", s, obj.id().inner_id(), obj.tags().get("name").map(|x| x.as_str()).unwrap_or("<NO NAME>"))
	}
}

fn main() -> anyhow::Result<()> {
   rayon::ThreadPoolBuilder::new()
	  .num_threads(std::thread::available_parallelism()?.into())
	  .build_global()?;

   unsafe { std::env::set_var("RUST_BACKTRACE", "full"); };

	let args = std::env::args();
	let last = args.last().unwrap();

	if last == "--cache" {
		cache()?;
		return Ok(());
	}

	if last == "--pipeline1" {
		pipeline1()?;
		return Ok(());
	}

	eprintln!("wrong arg");

	Ok(())
}

type SyncPlace = Arc<RwLock<Place>>;
structstruck::strike! {
	#[strikethrough[derive(Debug, Clone)]]
	// #[strikethrough[serde(rename_all = "snake_case")]]
	struct Place {
		mapped_type: #[derive(PartialEq, PartialOrd, Eq, Ord)] enum {
			City,
			Region, 
			Country
		},
		parents: Vec<SyncPlace>,
		geometry: Geometry,
		tags: HashMap<String, String>,
		center: geo::Coord,
		source: OsmObj
	}
}

// #[derive(Debug, Clone)]
// struct PlacePtr(Box<*const Place>);
// impl Serialize for PlacePtr {
// 	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
// 		let ptr = self.0;
// 		Arc::
// 		let v = unsafe { std::ptr::read(ptr) };
// 		Place::serialize(&v, serializer)
// 	}
// }
// unsafe impl Send for PlacePtr {}
// unsafe impl Sync for PlacePtr {}

impl Place {
	fn name(&self) -> &String {
		&self.tags["name"]
	}
}

fn ways_to_way_nodes<F: Fn(&Way) -> bool + Sync>(elements: &Vec<OsmObj>, way_filter: F) -> HashMap<Way, Vec<Node>> {
	let ways_to_node_ids = elements.par_iter().filter_map(|el| {
		let way = el.way()?;
		way_filter(&way).then_some((way, way.nodes.clone()))
	}).collect::<HashMap<_, _>>();

	let node_ids_to_nodes = {
		let needed_nodes_ids = ways_to_node_ids.values().flatten().collect::<HashSet<_>>();

		elements.par_iter().filter_map(|el| {
			let node = el.node().filter(|node| needed_nodes_ids.contains(&node.id)).cloned()?;
			(node.id, node).into()
		}).collect::<HashMap<_, _>>()
	};

	let ways_to_nodes = ways_to_node_ids.into_par_iter().map(|(way, node_ids)| {
		let nodes = node_ids.into_par_iter().filter_map(|n| node_ids_to_nodes.get(&n).cloned()).collect::<Vec<_>>();
		(way.clone(), nodes)
	}).collect::<HashMap<_, _>>();

	ways_to_nodes
}

fn relations_to_relation_nodes<F: Fn(&Relation) -> bool + Sync>(elements: &Vec<OsmObj>, rel_filter: F) -> HashMap<Relation, Vec<Vec<Node>>> {
	let way_ids = Mutex::new(HashSet::with_capacity(10_000));

	let relations_to_way_ids = elements.par_iter().filter_map(|el| {
		let relation = el.relation().cloned().filter(|rel| rel_filter(rel))?;
		let relation_way_ids = relation.refs.iter().filter_map(|rref| rref.member.way().inspect(|way_id| { way_ids.lock().insert(way_id.clone()); })).collect::<Vec<_>>();
		return Some((relation, relation_way_ids));
	}).collect::<HashMap<_, _>>();

	let ways_to_nodes = ways_to_way_nodes(&elements, |way| way_ids.lock().contains(&way.id))
		.into_par_iter()
		.map(|(way, nodes)| (way.id, nodes))
		.collect::<HashMap<_, _>>();

	let result = relations_to_way_ids.into_par_iter().map(|(relation, way_ids)| {
		(relation, way_ids.iter().filter_map(|wid| ways_to_nodes.get(wid).cloned()).collect())
	}).collect::<HashMap<_, _>>();

	result
} 

fn build_polygon(r: Vec<Vec<Node>>) -> Option<geo::Geometry> {
	let coords = merge_nodes(r.to_vec())
		.into_iter()	
		.map(|wn| wn.iter().map(|node| geo::Coord { x: node.lon(), y: node.lat() }).collect::<Vec<_>>())
		.collect::<Vec<_>>();

	let polygons = coords.iter().filter_map(|points| {
		let exterior = geo::LineString(points.to_vec());
		if exterior.is_empty() { return None }
		geo::Polygon::new(exterior, vec![]).into()
	}).collect::<Vec<_>>();

	if polygons.is_empty() { return None; }

	let geometry = if polygons.len() == 1 { 
		Geometry::Polygon(polygons[0].clone()) 
	}
	else { 
		Geometry::MultiPolygon(geo::MultiPolygon(polygons.clone()))
	};

	Some(geometry)
}


fn pipeline1() -> anyhow::Result<()> {
	let osm_src = std::env::var("OSM_SRC")?;

	let mut pbf = OsmPbfReader::new(File::open(Path::new(&osm_src))?);

	let artifacts_dir = PathBuf::from(format!("./output/pipeline1_{}", Local::now().format("%d_%m_%H_%M_%S")));
	std::fs::create_dir_all(&artifacts_dir);

	println!("loading elements in memory");
	let elements = pbf.par_iter()
		.filter_map(|el| el.ok())
		.collect::<Vec<_>>();

	// https://wiki.openstreetmap.org/wiki/Key:place
	// https://wiki.openstreetmap.org/wiki/Places
	// https://wiki.openstreetmap.org/wiki/Key:admin_level
	
	println!("finding country nodes");
	let countries_nodes = elements.par_iter()
		.filter_map(|el| el.node().filter(|n| n.tags.contains("place", "country")).cloned())
		.collect::<Vec<_>>();

	let relations_source_nodes = Mutex::new(HashMap::with_capacity(10_000));

	println!("mapping country_nodes to relation/nodes");
	let countries_relations = {
		let nodes_hs = countries_nodes.par_iter().map(|rn| rn.id).collect::<HashSet<_>>();
		let result = relations_to_relation_nodes(&elements, |rel|  {
			let Some(name) = rel.tags.get("name") else { return false };
			// if name.contains("land mass") { return false; }

			return rel.refs.iter().find_map(|rf| rf.member.node().filter(|nid| nodes_hs.contains(&nid))).inspect(|nid| {
				relations_source_nodes.lock().insert(rel.clone(), nid.clone());
			}).is_some();
		});
		result
	};

	{
		let mut cn = countries_nodes.iter()
			.filter_map(|x| x.url().into())
			.collect::<Vec<_>>();
		let mut cr = countries_relations.iter()
			.filter_map(|x| format!("{} / {}", x.0.tags.get("name:en")?.to_string(), x.0.url()).into())
			.collect::<Vec<_>>();
		cn.sort_unstable();
		cr.sort_unstable();
		
		std::fs::write(artifacts_dir.join("countries_nodes.txt"), cn.join("\n"));
		std::fs::write(artifacts_dir.join("countries_relations.txt"), cr.join("\n"));	
	}

	println!("finding regions nodes");
	let regions_nodes = elements.par_iter()
		.filter_map(|el| el.node().filter(|n| n.tags.get("name").is_some() && n.tags.get("place").is_some_and(|place| ["region", "state", "province"].contains(&place.as_str()))).cloned())
		.collect::<Vec<_>>();

	println!("mapping regions to relation/nodes");
	let regions_relations = {
		let nodes_hs = regions_nodes.par_iter().map(|rn| rn.id).collect::<HashSet<_>>();
		relations_to_relation_nodes(&elements, |rel| {
			let Some(name) = rel.tags.get("name") else { return false };

			return rel.refs.iter().find_map(|rf| rf.member.node().filter(|nid| nodes_hs.contains(&nid))).inspect(|nid| {
				relations_source_nodes.lock().insert(rel.clone(), nid.clone());
			}).is_some();
		})
	};

	println!("regions_nodes: {}, regions_relations: {}", regions_nodes.len(), regions_relations.len());
	{
		let mut rn = regions_nodes.iter()
			.map(|x| x.tags.get("name:en").or_else(|| x.tags.get("name")).unwrap().to_string())
			.collect::<Vec<_>>();
		let mut rr = regions_relations.iter()
			.map(|x| x.0.tags.get("name:en").or_else(|| x.0.tags.get("name")).unwrap().to_string())
			.collect::<Vec<_>>();

		rn.sort_unstable();
		rr.sort_unstable();
		
		std::fs::write(artifacts_dir.join("regions_nodes.txt"), rn.join("\n"));
		std::fs::write(artifacts_dir.join("regions_relations.txt"), rr.join("\n"));	
	}


	let cities_nodes = elements.par_iter()
		.filter_map(|el| 
			el.node()
				.filter(|n| n.tags.contains_key("name"))
				.filter(|n| n.tags.get("place").is_some_and(|place| ["city", "town"].contains(&place.as_str())))
				.cloned()
		)
		.collect::<Vec<_>>();

	println!("found {} cities", cities_nodes.len());
	std::fs::write(artifacts_dir.join("cities_nodes.txt"), cities_nodes.par_iter().map(|x| x.tags.get("name:en").or_else(|| x.tags.get("name")).unwrap().to_string()).collect::<Vec<_>>().join("\n"));


	println!("loading center_nodes");
	let relations_source_nodes = {
		let f = relations_source_nodes.into_inner();
		let node_ids = f.values().collect::<Vec<_>>();
		let node_ids_to_nodes = elements
			.par_iter()
			.filter_map(|el| el.node().filter(|n| node_ids.contains(&&n.id)).map(|n| (n.id, n)))
			.collect::<HashMap<_, _>>();

		f.into_iter().map(|(rel, nid)| (rel, node_ids_to_nodes[&nid])).collect::<HashMap<_, _>>()
	};


	println!("building places from countries");
	let places_countries = countries_relations.into_par_iter().filter_map(|(rel, nnodes)| {
		if rel.tags.get("name").is_none() {
			println!("country wo name: {}", rel.id.0);
			return None;
		}
		if rel.tags.get("name").unwrap() == "United States" {
			let geometry = build_polygon(nnodes.clone())?;
			
			std::fs::write(artifacts_dir.join("us.json"), geojson::GeoJson::Feature(geojson::Feature { 
				geometry: geojson::Geometry::from(match geometry { 
					Geometry::MultiPolygon(ref mp) => mp, 
					_ => unreachable!() 
				}).into(),
				..Default::default()
			}).to_string());
			println!("written debug us");
		}

		let source_node = relations_source_nodes[&rel];
		Place {
			mapped_type: MappedType::Country,
			parents: vec![],
			geometry: build_polygon(nnodes)?,
			center:  geo::Coord { x: source_node.lon(), y: source_node.lat() },
			tags: rel.tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
			source: rel.into(),
		}.into()
	}).collect::<Vec<_>>();


	println!("building places from regions");
	let places_regions = regions_relations.into_par_iter().filter_map(|(rel, nnodes)| {
		if rel.tags.get("name").is_none() {
			println!("region wo name: {}", rel.id.0);
			return None;
		}

		let source_node =  relations_source_nodes[&rel];
		Place {
			mapped_type: MappedType::Region,
			parents: vec![],
			geometry: build_polygon(nnodes)?,
			center:  geo::Coord { x: source_node.lon(), y: source_node.lat() },
			tags: rel.tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
			source: rel.into()
		}.into()
	}).collect::<Vec<_>>();

	println!("building places from cities");
	let places_cities = cities_nodes.into_par_iter().filter_map(|node| {
		if node.tags.get("name").is_none() {
			println!("city wo name: {}", node.id.0);
			return None;
		}
		Place {
			mapped_type: MappedType::City,
			parents: vec![],
			geometry: geo::Geometry::Point(geo::Point(geo::Coord { x: node.lon(), y: node.lat() })),
			center:  geo::Coord { x: node.lon(), y: node.lat() },
			tags: node.tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
			source: node.into(),
		}.into()
	}).collect::<Vec<_>>();

	let places_collections = [ places_countries, places_regions, places_cities, ].into_iter()
		.map(|c| c.into_iter().map(|p| Arc::new(RwLock::new(p))).collect::<Vec<_>>() )
		.collect::<Vec<_>>();

	println!("building graph");

	let find_parent_places = |place: SyncPlace, parent_collection: &Vec<SyncPlace>| -> Vec<SyncPlace>  {
		let mut parents = Vec::new();
		for container in parent_collection.iter() {
			if container.read().geometry.contains(&place.read().center) {
				parents.push(Arc::clone(&container));
				// break;
			}
		}
		parents
	};
	
	let places_to_delete = RwLock::new(Vec::new());

	let bar = ProgressBar::new(places_collections.iter().skip(1).flatten().count() as u64);
	bar.enable_steady_tick(Duration::from_millis(200));
	
	for (i, collection) in places_collections.iter().enumerate().skip(1) {
		collection.par_iter().for_each(|place| {
			for j in (0..i).rev() {
				let mut parents = find_parent_places(Arc::clone(&place), &places_collections[j]);
				parents.retain(|p| !places_to_delete.read().contains(&p.read().source.str_id()));
				
				if parents.len() > 0 {
					place.write().parents.append(&mut parents);
					break;
				}
			}

			let place = place.read();
			if place.parents.is_empty() {
				eprintln!("no parents were found for place {} {}", place.name(), place.source.url());
				places_to_delete.write().push(place.source.str_id());
			}

			bar.inc(1);
		});
	}

	let mut places = places_collections.into_iter().flatten().collect::<Vec<_>>();
	let places_to_delete = places_to_delete.into_inner();
	
	places.retain(|p| !places_to_delete.contains(&p.read().source.str_id()));
	drop(places_to_delete);


	use std::{fs::OpenOptions, io::{BufWriter, Write}, path::PathBuf};
	let mut file = BufWriter::new(OpenOptions::new().create(true).append(true).open(&artifacts_dir.join("table.csv"))?);
	file.write("country_en,country_ru,region_en,region_ru,city_en,city_ru,lon,lat,population\n".as_bytes())?;
	println!("writing table");

	for place in places.into_iter() {
		#[derive(Default)]
		struct Row {
			country_en : Option<String>,
			country_ru : Option<String>,
			region_en  : Option<String>,
			region_ru  : Option<String>,
			city_en    : Option<String>,
			city_ru    : Option<String>,
		}
		impl Row {
			fn get_names(item: &SyncPlace) -> (Option<String>, Option<String>) {
				let tags = &item.read().tags;
				(tags.get("name:en").or(tags.get("name")).map(|x| x.to_string()), tags.get("name:ru").map(|x| x.to_string()))
			}
			fn traverse(&mut self, item: SyncPlace) {
				match item.read().mapped_type {
					MappedType::Country => {
						(self.country_en, self.country_ru) = Row::get_names(&item)
					},
					MappedType::Region => {
						(self.region_en, self.region_ru) = Row::get_names(&item)
					}
					MappedType::City => {
						(self.city_en, self.city_ru) = Row::get_names(&item)
					},
				}
				for parent in item.read().parents.iter() {
					self.traverse(parent.clone())
				}
			}
		}
		let mut row = Row::default();
		row.traverse(place.clone());

		const STRING_EMPTY: String = String::new();
		let center = place.read().geometry.centroid().unwrap();

		let s = format!("{country_en},{country_ru},{region_en},{region_ru},{city_en},{city_ru},{lon},{lat},{population}\n", 
			country_en = row.country_en.unwrap_or(STRING_EMPTY),
			country_ru = row.country_ru.unwrap_or(STRING_EMPTY),
			region_en  = row.region_en.unwrap_or(STRING_EMPTY),
			region_ru  = row.region_ru.unwrap_or(STRING_EMPTY),
			city_en    = row.city_en.unwrap_or(STRING_EMPTY),
			city_ru    = row.city_ru.unwrap_or(STRING_EMPTY),
			lon        = center.x(),
			lat        = center.y(),
			population = place.read().tags.get("population").cloned().unwrap_or(STRING_EMPTY)
		);
		file.write(s.as_bytes())?;
	}

	Ok(())
}


fn find_centers(pbf: &mut OsmPbfReader<File>, places: &mut Vec<Place>) {
	println!("searching for places centers");

	println!("getting nodes list");
	pbf.rewind();
	let nodes_ids_to_nodes = pbf.par_iter().filter_map(|el| (el.ok()?.node().cloned())).map(|n| (n.id.0, n)).collect::<HashMap<_, _>>();

	let found_centers_count = AtomicU64::new(0);

	let bar = ProgressBar::new(places.len() as u64);
	bar.enable_steady_tick(Duration::from_millis(200));

	places.par_iter_mut().for_each(|place| {
		bar.inc(1);

		let mut center = Option::None;
		let refs = match place.source {
			OsmObj::Relation(ref rel) => rel.refs.clone(),
			_ => vec![]
		};
		if let Some((center_node_id, role)) = refs.iter().find_map(
			|rf| rf.member.node()
				.filter(|_| ["capital", "admin_center", "admin_centre", "label"].contains(&rf.role.as_str()))
				.map(|n| (n, rf.role.clone()))
			) {
			if let Some(node) = nodes_ids_to_nodes.get(&center_node_id.0) {
				place.tags.insert("center_role".into(), format!("role_{}", role).into());
				center = Some(geo::Coord {x: node.lon(), y: node.lat()});
				found_centers_count.fetch_add(1, Ordering::Relaxed);
			}
		};

		if center.is_none() {
			for node in nodes_ids_to_nodes.values() {
				if node.tags.get("name").is_some_and(|node_name| node_name == place.name()) {
					let c = geo::Coord { x: node.lon(), y: node.lat() };
					if place.geometry.contains(&c) {
						place.tags.insert("center_role".into(), "name".into());
						center = Some(c);
						found_centers_count.fetch_add(1, Ordering::Relaxed);
						break;
					}
				}
			}
		};

		if center.is_none() && let Some(centroid) = geo::Centroid::centroid(&place.geometry) {
			center = Some(centroid.into());
			place.tags.insert("center_role".into(), "centroid".into());
		}

		if center.is_none() {
			let start = geo::CoordsIter::coords_iter(&place.geometry).next().unwrap();
			place.tags.insert("center_role".into(), "start".into());
			center = Some(start);
		}
	
		let center = center.unwrap();
		place.tags.insert("center".into(), format!("{},{}", center.x, center.y).into());
	});

	bar.finish_and_clear();
}

static PLACES: &[&str] = &["country", "state", "region", "city", "town", "village"];
static ADMIN_LEVELS: &[&str] = &["2","3","4","8","9"];
static MEMBER_ROLES: &[&str] = &["label", "admin_centre", "capital"];

fn has_admin_tags(tags: &Tags) -> bool {
	tags.get("admin_level").is_some_and(|al| ADMIN_LEVELS.contains(&al.as_str())) 
		|| tags.get("place").is_some_and(|place| PLACES.contains(&place.as_str())) 
		|| tags.contains_key("capital") 
		|| tags.contains("boundary", "administrative") 
}

static TAGS_TO_RETAIN: &[&str] = &["name", "name_en", "admin_level", "place", "capital", "population", "boundary"];
fn filter_tags(tags: &mut Tags) -> &Tags {
	tags.retain(|k, _| TAGS_TO_RETAIN.contains(&k.as_str()));
	tags
}

fn cache() -> anyhow::Result<()> { 
	use osm_io::osm::{
	   model::element::Element,
	   pbf::{compression_type::CompressionType, file_info::FileInfo, parallel_writer::ParallelWriter},
	   pbf::reader::Reader
	};
 
	let osm_src = std::env::var("OSM_SRC")?;
	let mut pbf = OsmPbfReader::new(File::open(Path::new(&osm_src)).unwrap());
	
	println!("counting elements");
	let element_count = pbf.par_iter().count() as u64;
	let bar = ProgressBar::new(element_count);
	bar.enable_steady_tick(Duration::from_millis(200));
 
	println!("parsing relations...");
	pbf.rewind()?;
	// taking all relevant relations
	let relations = pbf.par_iter().filter_map(|obj| {
		bar.inc(1);

		let rel = obj.ok().and_then(|o| o.relation().cloned())?;
		
		has_admin_tags(&rel.tags).then_some(rel)
	}).collect::<Vec<_>>();
 
	println!("finished filtering {} relations!", relations.len());
	
	let relations_way_ids 	= HashSet::<i64>::from_iter(relations.iter().map(|rel| rel.refs.iter().filter_map(|r| r.member.way().map(|id| id.0)).collect::<Vec<_>>()).flatten());
	let relations_nodes_ids = HashSet::<i64>::from_iter(relations.iter().map(
	   |rel| rel.refs.iter().filter_map(|r| r.member.node().filter(|_| MEMBER_ROLES.contains(&r.role.as_str())).map(|id| id.0)).collect::<Vec<_>>()
	).flatten());
	let relations_ids = relations.into_iter().map(|rel| rel.id.0).collect::<HashSet<_>>();
 
 
	println!("parsing ways...");
	bar.reset();
	pbf.rewind()?;
	let ways = pbf.par_iter().filter_map(|obj| {
		bar.inc(1);

		let way = obj.ok().and_then(|o| o.way().cloned())?;

		(relations_way_ids.contains(&way.id.0) || has_admin_tags(&way.tags)).then_some(way)
	}).collect::<Vec<_>>();
	println!("finished parsing {} ways", ways.len());
	let ways_nodes_ids = HashSet::<i64>::from_iter(ways.iter().map(|w| w.nodes.clone().iter().map(|id| id.0).collect::<Vec<i64>>()).flatten());
	let ways_ids = ways.into_iter().map(|way| way.id.0).collect::<HashSet<_>>();
 
 
	println!("filtering nodes...");
	bar.reset();
	pbf.rewind()?;
	let nodes_ids = pbf.par_iter().filter_map(|obj| {
		bar.inc(1);
		let node = obj.ok().and_then(|o| o.node().cloned())?;
		(relations_nodes_ids.contains(&node.id.0) 
			|| ways_nodes_ids.contains(&node.id.0) 
			|| has_admin_tags(&node.tags)
		).then_some(node.id.0)
	}).collect::<HashSet<_>>();
	println!("finished parsing {} nodes", nodes_ids.len());
 
	let mut out_name = PathBuf::from_str(&osm_src)?; 
	out_name.set_file_name(
		format!(
			"{}_cache_{}.pbf", 
			out_name.file_stem().unwrap().to_string_lossy(), 
			Local::now().format("%d_%m_%H_%M")
	));
	// https://github.com/navigatorsguild/osm-io/blob/main/examples/parallel-pbf-io.rs#L22
	
	let tasks_count = std::thread::available_parallelism()?.into();

	let mut file_info = FileInfo::default(); file_info.with_writingprogram_str("planet-filtered");
	let mut writer = ParallelWriter::from_file_info(tasks_count * 8000 * 32, 8000, out_name.clone(), file_info, CompressionType::Zlib).unwrap();
	writer.write_header().unwrap();
 
	println!("rewriting elements from source file");
	let writer = Arc::new(Mutex::new(writer));
	let shared_writer = writer.clone();
	let shared_bar = bar.clone();

	Reader::new(&PathBuf::from(&osm_src))?.parallel_for_each(tasks_count, move |element| {

		let mut should_inc = true;
		let preserve = match element {
			Element::Relation { ref relation } 	=> relations_ids.contains(&relation.id()),
			Element::Node { ref node } 			=> nodes_ids.contains(&node.id()),
			Element::Way { ref way } 			=> ways_ids.contains(&way.id()),
			_ => { 
				should_inc = false;
				false
			},
		};

		if preserve { shared_writer.lock().write_element(element.clone())?; }
		if should_inc { shared_bar.inc(1); }
		Ok(())
	})?;
	
	writer.lock().close()?;
	bar.finish_and_clear();
	
	println!("cached version path: {out_name:#?}");

	Ok(())
 }
 
