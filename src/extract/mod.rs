mod converter;
use std::{
   collections::{HashMap, HashSet}, fs::{File, OpenOptions}, io::Write, iter::FromIterator, ops::Deref, path::Path, str::FromStr, sync::{atomic::{AtomicU32, AtomicU64, Ordering}, Arc, Mutex}, time::{SystemTime, UNIX_EPOCH}
};

use converter::{convert_nodes_to_points, merge_nodes};
use geo::{Centroid, CoordsIter, HasDimensions};
use geojson::Feature;
use indicatif::ProgressBar;
use osm_io::osm::model::element;
use osmpbfreader::{Node, NodeId, OsmPbfReader, Ref, Relation, RelationId, WayId};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use smartstring::SmartString;

pub fn extract_polygons(pbf_path: &str) -> anyhow::Result<()> {
   let mut pbf = OsmPbfReader::new(File::open(Path::new(pbf_path)).unwrap());

   let output_base_dir = std::path::PathBuf::from_str(&format!("./extracted/al234.extract_full_{}", SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()))?;
   std::fs::create_dir_all(&output_base_dir)?;

   let elements         = pbf.par_iter().map(Result::unwrap).collect::<Vec<_>>();
   let elements_count   = elements.len();
   println!("loaded {elements_count} OSM elements in memory");

   let recorded_features = AtomicU32::new(0);
   let centers_found = AtomicU32::new(0);
   
   struct CachedRelation<'a> {
      pub name: &'a SmartString<smartstring::LazyCompact>,
      pub refs: Vec<Ref>,
      pub rel_id: i64
   }

   let write_feature = |feature: Feature| -> anyhow::Result<()> {
      let mut safe_name = match feature.id.clone().unwrap() { geojson::feature::Id::String(s) => s, _ => unreachable!() };
      safe_name.retain(|c| !r#"\\&:<>|*"#.contains(c));
      safe_name = safe_name.replace("/", "_");
      let file_name = format!("{}.geojson", safe_name);
      std::fs::write(output_base_dir.join(&file_name), feature.to_string())?;
      recorded_features.fetch_add(1, Ordering::Relaxed);
      Ok(())
   };
   
   let mut cached_relations = Arc::new(Mutex::new(Vec::with_capacity(elements_count / 10)));
   
   {
      println!("analyzing relations");
      let relations_to_way_ids = elements.par_iter().filter_map(|el| {
         let relation = el.relation()?;
         let name = relation.tags.get("name")?;
         let has_right_tags = relation.tags.get("place").is_some_and(|pl| ["city", "town"].contains(&pl.as_str())) 
            || relation.tags.get("admin_level").is_some_and(|al| [2,3,4,8].contains(&al.parse::<u8>().unwrap_or(0)));
            
         if !has_right_tags { return None; }

         let relation_way_ids = relation.refs.iter().filter_map(|rref| rref.member.way().map(|way| way.0)).collect::<Vec<_>>();
         return Some((relation, relation_way_ids)); 
      }).collect::<HashMap<_, _>>();

      let ways_ids_to_node_ids = {
         println!("getting nodes ids of each relation way");
         let needed_way_ids = relations_to_way_ids.iter()
            .map(|rw| rw.1)
            .flatten().collect::<HashSet<_>>();

         elements.par_iter().filter_map(|el| {
            let way = el.way().filter(|w| needed_way_ids.contains(&w.id.0))?;
            Some((way.id.0, way.nodes.iter().map(|n| n.0).collect::<Vec<_>>()))
         }).collect::<HashMap<_, _>>()
      };

      let node_ids_to_nodes = {
         let needed_nodes_ids = ways_ids_to_node_ids.values().flatten().collect::<HashSet<_>>();

         println!("getting nodes of each relation way");
         elements.par_iter().filter_map(|el| {
            let node = el.node().filter(|node| { 
               needed_nodes_ids.contains(&node.id.0)
            })?;
            Some((node.id.0, node))
         }).collect::<HashMap<_, _>>()
      };

      println!("mapping relations to nodes");
      let realtions_to_ways_nodes = relations_to_way_ids.par_iter().map(|(rel, way_ids)| {

         let nodes = ways_ids_to_node_ids.iter().filter_map(|(way_id, node_ids)| {
         if !way_ids.contains(way_id) { return None; }

         node_ids.iter()
               .filter_map(|node_id| node_ids_to_nodes.get(node_id))
               .collect::<Vec<_>>().into()

         }).collect::<Vec<_>>();

         return (rel, nodes)
         
      }).collect::<HashMap<_, _>>();

      let pb = ProgressBar::new(realtions_to_ways_nodes.len().try_into().unwrap());
      realtions_to_ways_nodes.par_iter().try_for_each(|(relation, nnodes)| -> anyhow::Result<()> {
         pb.inc(1);

         let name = relation.tags.get("name").unwrap();
         let mut feature = None;

         let id         = format!("relation/{}/{}", relation.id.0, &name);
         let url        = format!("https://www.openstreetmap.org/relation/{}", relation.id.0);

         let coords = nnodes.into_iter().map(|wn| 
            wn.iter().map(|node| geo::Coord { x: node.lon(), y: node.lat() }).collect::<Vec<_>>()
         ).collect::<Vec<_>>();

         let polygons = coords.iter().map(|points| geo::Polygon::new(geo::LineString(points.to_vec()), vec![])).collect::<Vec<_>>();
         if polygons.is_empty() {
            // eprintln!("skipping empty polygon {id} {url}");
            return Ok(());
         }
         let geo_geometry = (polygons.len() == 1)
            .then(||             geo::Geometry::Polygon(polygons[0].clone()))
            .unwrap_or(geo::Geometry::MultiPolygon(geo::MultiPolygon(polygons)));

         let mut properties = geojson::JsonObject::from_iter( 
            relation.tags.iter().map(|(key, value)| (key.to_string(), serde_json::to_value(value).unwrap()))
         );
         properties.insert("osm_url".to_owned(), serde_json::to_value(url)?);

         let mut center_point;
         let center_node = relation.refs.iter().find_map(|rf|
            rf.member.node()
               .filter(|node| ["capital", "admin_center", "label"].contains(&rf.role.as_str()))
               .map(|n| (n, rf.role.clone()))
         );
         if let Some((center_node_id, role)) = center_node && let Some(node) = elements.iter().find_map(|el| el.node().filter(|n| n.id == center_node_id)) {
            center_point = geo::Point::new(node.lon(), node.lat());
            properties.insert("center_role".to_owned(), role.to_string().into());
            centers_found.fetch_add(1, Ordering::Relaxed);
         }
         else {
            center_point = geo_geometry.centroid()
               .inspect(|_| { properties.insert("center_role".to_owned(), "centroid".into()); })
               .unwrap_or_else(|| {
                  let start = geo_geometry.coords_iter().collect::<Vec<_>>()[0];
                  properties.insert("center_role".to_owned(), "start".into());
                  geo::Point(start)
               });
         }
         properties.insert("center".into(), format!("{},{}", center_point.x(), center_point.y()).into());
         feature = Some(Feature {
            id:         geojson::feature::Id::String(id).into(),
            properties: properties.into(),
            geometry:   geojson::Geometry::try_from(&geo_geometry).ok(),
            ..Default::default()
         });
         
         cached_relations.lock().unwrap().push(CachedRelation { 
            name,
            rel_id: relation.id.0,
            refs: relation.refs.clone(),
         });
         
         if let Some(feature) = feature { write_feature(feature)?; }

         Ok(())
      })?;
   }

   {
      println!("analyzing ways");
      let cached_relations = cached_relations.lock().unwrap();

      let ways_to_node_ids = elements.par_iter().filter_map(|el| {
         let way = el.way()?;
         let name = way.tags.get("name")?;

         let has_right_tags = way.tags.get("place").is_some_and(|pl| ["city", "town"].contains(&pl.as_str()));
            || way.tags.get("admin_level").is_some_and(|al| [2,3,4,8,9].contains(&al.parse::<u8>().unwrap_or(0)));
            
         if !has_right_tags { return None; }

         let parent_relations = cached_relations.iter().filter(|rel|
            rel.refs.iter().any(|rref| rref.member.way().is_some_and(|w| w == way.id))
         ).collect::<Vec<_>>();

         for parent_relation in parent_relations {
            if parent_relation.name == name {
               println!("skipping way {} as there is parent relation {}",
                  format!("https://www.openstreetmap.org/way/{}", way.id.0), 
                  format!("https://www.openstreetmap.org/relation/{}", parent_relation.rel_id)
               );
               return None;
            }
         }
         
         return Some((way, way.nodes.clone())); 
      }).collect::<HashMap<_, _>>();

      let node_ids_to_nodes = {
         let needed_nodes_ids = ways_to_node_ids.values().flatten().collect::<HashSet<_>>();
         println!("getting nodes of each way");
         elements.par_iter().filter_map(|el| {
            let node = el.node().filter(|node| { needed_nodes_ids.contains(&node.id) })?;
            Some((node.id, node))
         }).collect::<HashMap<_, _>>()
      };

      let ways_to_nodes = ways_to_node_ids.par_iter().map(|(rel, node_ids)| {
         let nodes = node_ids.iter().filter_map(|n| node_ids_to_nodes.get(n)).collect::<Vec<_>>();
         (rel, nodes)
      }).collect::<HashMap<_, _>>();
      
      let pb = ProgressBar::new(ways_to_node_ids.len().try_into().unwrap());
      ways_to_nodes.par_iter().try_for_each(|(way, nodes)| -> anyhow::Result<()> {
         pb.inc(1);

         let name = way.tags.get("name").unwrap();
         let mut feature = None;

         let id   = format!("way/{}/{}", way.id.0, &name);
         let url  = format!("https://www.openstreetmap.org/way/{}", way.id.0);

         
         let exterior = geo::LineString(nodes.into_iter().map(|node| geo::Coord { x: node.lon(), y: node.lat() }).collect::<Vec<_>>());

         if exterior.is_empty() {
            eprintln!("skipping empty polygon {id} {url}");
            return Ok(());
         }
         if !exterior.is_closed() {
            eprintln!("skipping non-closed polygon {id} {url}");
            return Ok(())
         }

         let polygon = geo::Polygon::new(exterior, vec![]);
         let geo_geometry = geo::Geometry::Polygon(polygon);

         let mut properties = geojson::JsonObject::from_iter( 
            way.tags.iter().map(|(key, value)| (key.to_string(), serde_json::to_value(value).unwrap()))
         );
         properties.insert("osm_url".to_owned(), serde_json::to_value(url)?);
         
         let center_point = geo_geometry.centroid()
            .inspect(|_| { properties.insert("center_role".to_owned(), "centroid".into()); })
            .unwrap_or_else(|| {
               let start = geo_geometry.coords_iter().collect::<Vec<_>>()[0];
               properties.insert("center_role".to_owned(), "start".into());
               geo::Point(start)
            });
         properties.insert("center".into(), format!("{},{}", center_point.x(), center_point.y()).into());

         feature = Some(Feature {
            id:         geojson::feature::Id::String(id).into(),
            properties: properties.into(),
            geometry:   geojson::Geometry::try_from(&geo_geometry).ok(),
            ..Default::default()
         });

         if let Some(feature) = feature { write_feature(feature)?; }

         Ok(())
      })?;
   }
   println!("Done. Output: {output_base_dir:#?}, {}/{} correct centers were found", centers_found.load(Ordering::Relaxed), recorded_features.load(Ordering::Relaxed));

   Ok(())
}

#[derive(Clone)]
pub struct RelationNodes {
   pub relation: Relation,
   pub nodes: Vec<Vec<Node>>,
}

// pub fn are_tags_right(tags: &Vec<osm_io::osm::model::tag::Tag>) -> bool {
//    for tag in tags {
//       if tag.k() == "place" && ["city", "town"].contains(&tag.v().as_str()) {
//          return true;
//       }
//       if tag.k() == "admin_level" {
//          return true;
//       }
//    }
//    false
// }

// pub fn are_tags_right2(tags: &osmpbfreader::Tags) -> bool {
//    return tags.get("place").is_some_and(|pl| ["city", "town"].contains(&pl.as_str())) || 
//             tags.get("admin_level").is_some_and(|al| [2,3,4,8,9].contains(&al.parse::<u8>().unwrap_or(0)));
// }

pub fn cache(input: &str) -> anyhow::Result<()> {
   use std::{path::PathBuf, sync::{Arc, Mutex}};

   use osm_io::osm::{
      model::element::Element,
      pbf::{compression_type::CompressionType, file_info::FileInfo, parallel_writer::ParallelWriter},
   };

   let mut pbf = OsmPbfReader::new(File::open(Path::new(input)).unwrap());

   println!("parsing relations...");
   // taking all relevant relations
   let relations = pbf.par_iter().filter_map(|obj| {
      let rel = obj.ok().and_then(|o| o.relation().cloned())?;
      if rel.tags.contains("boundary", "administrative") 
      || rel.tags.get("place").is_some_and(|place| ["city", "town"].contains(&place.as_str())) 
      || rel.tags.contains_key("capital") 
      || rel.tags.get("admin_level").is_some_and(|al| [2,3,4,8].contains(&al.parse::<u8>().unwrap_or(0))) {
         return Some(rel);
      }
      return None;
   }).collect::<Vec<_>>();

   println!("finished filtering {} relations!", relations.len());
   

   let relations_way_ids:     HashSet<i64>   = HashSet::from_iter(relations.iter().map(|rel| rel.refs.iter().filter_map(|r| r.member.way().map(|id| id.0)).collect::<Vec<_>>()).flatten());
   let relations_nodes_ids:   HashSet<i64>   = HashSet::from_iter(relations.iter().map(
      |rel| rel.refs.iter().filter_map(|r| r.member.node().filter(|n| ["label", "admin_center", "capital"].contains(&r.role.as_str())).map(|id| id.0)).collect::<Vec<_>>()
   ).flatten());
   let relations_ids:         HashSet<i64>   = relations.into_iter().map(|rel| rel.id.0).collect::<HashSet<_>>();


   println!("parsing ways...");
   pbf.rewind()?;
   let ways = pbf.par_iter().filter_map(|obj| {
      let way = obj.ok().and_then(|o| o.way().cloned())?;
      if relations_way_ids.contains(&way.id.0) { return Some(way); }
      
      if way.tags.contains("boundary", "administrative")
      || way.tags.get("place").is_some_and(|pl| ["city", "town"].contains(&pl.as_str()))
      || way.tags.get("admin_level").is_some_and(|al| [2,3,4,8].contains(&al.parse::<u8>().unwrap_or(0))) {
         return Some(way);
      }
      return None;
   }).collect::<Vec<_>>();
   println!("finished parsing {} ways", ways.len());
   let ways_nodes_ids: HashSet<i64> = HashSet::from_iter(ways.iter().map(|w| w.nodes.clone().iter().map(|id| id.0).collect::<Vec<i64>>()).flatten());
   let ways_ids = ways.into_iter().map(|way| way.id.0).collect::<HashSet<_>>();


   println!("filtering nodes...");
   pbf.rewind()?;
   let nodes_ids = pbf.par_iter().filter_map(|obj| {
      let node = obj.ok().and_then(|o| o.node().cloned())?;
      (relations_nodes_ids.contains(&node.id.0) 
         || ways_nodes_ids.contains(&node.id.0) 
         || node.tags.get("place").is_some_and(|place| ["city", "town"].contains(&place.as_str())) 
         || node.tags.contains_key("capital") 
         || node.tags.get("admin_level").is_some_and(|al| [2,3,4,8].contains(&al.parse::<u8>().unwrap_or(0)))
      ).then_some(node.id.0)
   }).collect::<HashSet<_>>();
   println!("finished parsing {} nodes", nodes_ids.len());

   let reader = osm_io::osm::pbf::reader::Reader::new(&PathBuf::from(input)).unwrap();
   let mut file_info = FileInfo::default(); file_info.with_writingprogram_str("planet-filtered");
   let mut out_name = PathBuf::from_str(input)?; out_name.set_file_name(format!("{}_cache_{}.pbf", out_name.file_stem().unwrap().to_string_lossy(), SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()));
   // https://github.com/navigatorsguild/osm-io/blob/main/examples/parallel-pbf-io.rs#L22
   
   let tasks_count = std::thread::available_parallelism()?.into();
   let mut writer = ParallelWriter::from_file_info(tasks_count * 8000 * 32, 8000, out_name.clone(), file_info, CompressionType::Zlib).unwrap();
   writer.write_header().unwrap();

   let writer = Arc::new(Mutex::new(writer));
   let shared_writer = writer.clone();

   println!("rewriting elements from source file");
   let progress = AtomicU64::new(0);
   reader.parallel_for_each(tasks_count, move |element| {
      let preserve = match element {
         Element::Relation { ref relation } => relations_ids.contains(&relation.id()),
         Element::Node { ref node } => nodes_ids.contains(&node.id()),
         Element::Way { ref way } => ways_ids.contains(&way.id()) || relations_way_ids.contains(&way.id()),
         _ => false,
      };
      preserve.then(|| shared_writer.lock().unwrap().write_element(element.clone()).unwrap());
      let progress = progress.fetch_add(1, Ordering::Relaxed);
      (progress % 100_000 == 0).then(|| println!("writing progress: {progress} elements"));
      Ok(())
   }).unwrap();
   writer.lock().unwrap().close()?;
   println!("cached version path: {out_name:#?}");
   Ok(())
}
