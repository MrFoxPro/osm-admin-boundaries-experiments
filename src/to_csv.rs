use std::{collections::HashMap, fs::{self, OpenOptions}, io::{BufWriter, Write}};

use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::Deserialize;

// pub fn to_csv(source_dir: &str) -> anyhow::Result<()> {
// 	let dir = fs::read_dir(source_dir)?
// 		.map(Result::unwrap)
// 		.collect::<Vec<_>>();

// 	let dir_len = dir.len();

// 	let entires = dir.into_par_iter()
// 		.filter_map(|file|
// 			serde_json::from_reader::<_, RawGeoEntity>(fs::read(file.path()).ok()?.as_slice())
// 				.inspect_err(|err| eprintln!("failed to parse {:?}: {}", file.file_name(), err))
// 				.ok()
// 		)
// 		.collect::<Vec<_>>();

// 	println!("loaded {}/{}", entires.len(), dir_len);

// 	let entries = entires.into_iter().filter_map(|item| {
// 		let mut admin_level = item.properties.get("admin_level").and_then(|al| al.parse().ok());

// 		if admin_level.is_none() {
// 			if let Some(t) = item.properties.get("place") {
// 				admin_level = match t.as_str() {
// 					"country" => 2,
// 					"state" | "region" | "county" => 4,
// 					"city" | "town" | "village" => 8,
// 					_ => {
// 						println!("unknown place: {t} ({})", item.id);
// 						return None;
// 					}
// 				}.into()
// 			}
// 		} else if let Some(admin_level) = admin_level && ![2,4,8].contains(&admin_level) {
// 			eprintln!("{} admin_level: {}", item.id, admin_level);
// 		}

// 		Some(GeoEntity {id: item.id, admin_level: admin_level.unwrap(), geometry: item.geometry, properties: item.properties})
// 	}).collect::<Vec<_>>();

// 	let mut file = BufWriter::new(OpenOptions::new().create(true).append(true).open("./output/f.csv")?);
// 	file.write("al2,al4_3,place,population,lon,lat\n".as_bytes())?;
	
// 	for entry in entries {
// 		if ![9, 8, 2, 4].contains(&entry.admin_level) {
// 			continue;
// 		}
// 		let parent_admin_levels_to_find = [2, 3, 4].iter().filter(|x| **x < entry.admin_level).collect::<Vec<_>>();

// 	}

// 	Ok(())
// }