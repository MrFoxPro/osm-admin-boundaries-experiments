use geo::Coord;
use osmpbfreader::{Node, NodeId};

pub fn merge_nodes(nodes: Vec<Vec<Node>>) -> Vec<Vec<Node>> {
	/*
		merging of nodes is necessary because ways are split into multiple groups
		assumption:
		 - ways that can be attached to each other share one node at the end or beginning
		 - there are no three way intersections

		 1. start with first way and iterate over the rest of nodes and try to find a match
		   - if yes -> merge
		   - if no -> go to next
		 2. repeat process until nothing to merge
	*/

	let mut nodes = nodes.clone();
	let mut result_nodes = Vec::new();

	while !nodes.is_empty() {
		let mut path = nodes.swap_remove(0);

		loop {
			let matching_first = find_match(path.first().unwrap().id, &mut nodes);

			if let Some(mut matching_nodes) = matching_first {
				matching_nodes.reverse();
				matching_nodes.append(&mut path);
				path = matching_nodes;
				continue;
			}

			let matching_last = find_match(path.last().unwrap().id, &mut nodes);

			if let Some(mut matching_nodes) = matching_last {
				path.append(&mut matching_nodes);
				continue;
			}

			break;
		}

		result_nodes.push(path);
	}

   result_nodes
}

fn find_match(node_id: NodeId, nodes: &mut Vec<Vec<Node>>) -> Option<Vec<Node>> {
	/*
		n_id, [------, n_id-----, -----]
		=> Some(n_id-----), [------, -----]

		n_id, [------, -----n_id, -----]
		=> Some(n_id-----), [------, -----]
	*/
	for (i, node) in nodes.iter().enumerate() {
		if node.is_empty() {
			continue;
		}
		if node.first().unwrap().id == node_id {
			let result = nodes.swap_remove(i);
			return Some(result);
		}
		else if node.last().unwrap().id == node_id {
			let mut result = nodes.swap_remove(i);
			result.reverse();
			return Some(result);
		}
	}

	None
}

pub fn convert_nodes_to_points(nodes: &Vec<Node>) -> Vec<Coord> {
	nodes.iter().map(|node| geo::Coord { x: node.lon(), y: node.lat() }).collect()
}
