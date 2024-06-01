use osmpbfreader::{Node, NodeId, OsmPbfReader, Relation, RelationId, WayId};

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::iter::FromIterator;
use std::path::Path;
use std::time::Instant;

type OsmPbfReaderFile = osmpbfreader::OsmPbfReader<std::fs::File>;

#[derive(Clone)]
pub struct RelationNodes {
    pub relation: Relation,
    pub nodes: Vec<Vec<Node>>,
}

#[rustfmt::skip]
pub fn read_ways_and_relation(filename: &str, min_admin: u8, max_admin: u8, cache: bool) -> Vec<RelationNodes> {
    let mut pbf: OsmPbfReaderFile = OsmPbfReader::new(File::open(&Path::new(filename)).unwrap());
    let mut timer = Instant::now();

    println!("parsing relations...");
    let relation_id_to_relation = pbf.par_iter().filter_map(|obj| {
        let rel = obj.ok().and_then(|o| o.relation().cloned())?;

        let is_admin = rel.tags.contains("boundary", "administrative");
        let is_city = rel.tags.get("place")
            .is_some_and(|pl| ["city", "town"].contains(&pl.as_str()));

        let admin_level = rel.tags.get("admin_level")
            .and_then(|v| v.parse().ok())
            .unwrap_or(u8::MAX);

        (is_city || (is_admin && (min_admin..=max_admin).contains(&admin_level))).then(|| (rel.id, rel))
    }).collect::<HashMap<_, _>>();
    println!("finished parsing {} relations! {:#?}s", relation_id_to_relation.len(), timer.elapsed().as_secs());


    println!("parsing ways...");
    let relation_id_to_ways = relation_id_to_relation.iter()
        .map(|(rel_id, rel)| (*rel_id, rel.refs.iter().filter_map(|r| r.member.way()).collect()))
        .collect::<HashMap<RelationId, Vec<WayId>>>();

    timer = Instant::now();
    let way_ids: HashSet<WayId> = HashSet::from_iter(relation_id_to_ways.clone().into_values().into_iter().flatten());

    pbf.rewind().unwrap();
    let way_id_to_node_ids = pbf.par_iter().filter_map(|obj| {
        let way = obj.ok().and_then(|o| o.way().cloned())?;
        way_ids.contains(&way.id).then(|| (way.id, way.nodes))
    }).collect::<HashMap<_ ,_>>();

    println!("finished parsing {} ways! {:#?}s", way_id_to_node_ids.len(), timer.elapsed().as_secs());


    println!("parsing nodes...");
    timer = Instant::now();
    let node_ids: HashSet<NodeId> = HashSet::from_iter(way_id_to_node_ids.clone().into_values().flatten());

    pbf.rewind().unwrap();
    let node_id_to_node = pbf.par_iter().filter_map(|obj| {
        let node = obj.ok().and_then(|o| o.node().map(|n| n.clone()))?;
        node_ids.contains(&node.id).then(|| (node.id, node))
    }).collect::<HashMap<_, _>>();
    println!("finished parsing {} nodes! {:#?}s", node_id_to_node.len(), timer.elapsed().as_secs());


    if cache {
        use std::path::PathBuf;
        use std::sync::{Arc, Mutex};
        use std::thread::available_parallelism;
        use osm_io::osm::{
            model::element::Element,
            pbf::{compression_type::CompressionType, file_info::FileInfo, reader::Reader, writer::Writer},
        };

        let reader = Reader::new(&PathBuf::from("/home/foxpro/misc/osm/sources/planet-240520.osm.pbf")).unwrap();

        let mut file_info = FileInfo::default();
        file_info.with_writingprogram_str("planet-filtered");
        let mut writer = Writer::from_file_info(PathBuf::from("/home/foxpro/misc/osm/planet-240520_filtered3.pbf"), file_info, CompressionType::Zlib).unwrap();
        writer.write_header().unwrap();
    
        let writer = Arc::new(Mutex::new(writer));
        let (relation_id_to_relation, node_id_to_node, writer_clone) = (relation_id_to_relation.clone(), node_id_to_node.clone(), writer.clone());
        reader.parallel_for_each(available_parallelism().unwrap().into(), move |element| {
            let preserve = match element {
                Element::Relation { ref relation } => relation_id_to_relation.contains_key(&&RelationId(relation.id())),
                Element::Node { ref node } => node_id_to_node.contains_key(&&NodeId(node.id())),
                Element::Way { ref way } => way_ids.contains(&&WayId(way.id())),
                _ => false
            };

            if preserve {
                writer_clone.lock().unwrap().write_element(element.clone()).unwrap();
            }

            Ok(())
        }).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));
        println!("done caching, closing writer...");
        writer.lock().unwrap().close().unwrap();
    }

    relation_id_to_ways.iter().map(|(r_id, way_ids)| {
        let node_ids = way_ids
            .iter()
            .filter_map(|way_id| way_id_to_node_ids.get(way_id).cloned())
            .collect::<Vec<_>>();
        
        let nodes = node_ids.iter()
            .map(|node_ids| 
                node_ids.iter().filter_map(|node_id| node_id_to_node.get(node_id).cloned()).collect()
        ).collect();

        let relation = relation_id_to_relation.get(&r_id).unwrap().clone();
        RelationNodes { relation, nodes }
    }).collect()
}