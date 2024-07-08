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
