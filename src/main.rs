#![feature(let_chains)]
#![feature(string_remove_matches)]
#![allow(unused)]

mod eject_via_overpass;
mod eject_via_planet;
mod filter;
mod utils;
mod extract;

use eject_via_overpass::load_overpass_admin_places;
use filter::filter;

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread::{self};

use geo::{Contains, Coord, Intersects, Point};

use osm_io::osm::model::element::Element;
use osm_io::osm::pbf::reader::Reader as PbfReader;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

const KEY_NAME: &str = "name";
const KEY_NAME_RU: &str = "name:ru";
const KEY_POPULATION: &str = "population";
const KEY_OFFICIAL_NAME_RU: &str = "official_name:ru";

pub trait AdminRegion {
    fn geometry(&self) -> &geo::Geometry<f64>;
    fn name(&self) -> &String;
    fn admin_level(&self) -> u8;
    fn tags(&self) -> &HashMap<String, String>;

    fn try_get_ru_name(&self) -> String {
        let tags = self.tags();
        tags.get(KEY_NAME_RU).or_else(|| tags.get(KEY_OFFICIAL_NAME_RU)).unwrap_or_else(|| self.name()).to_string()
    }
}

fn find_places_by_name<'a, T>(places: &'a Vec<T>, lvl: u8, name: &'a String) -> Vec<&'a T> 
where T: AdminRegion + Sync
{
    let mut found = Vec::new();
    for p in places.iter() {
        if p.admin_level() != lvl { continue; }
        if p.name() == name { found.push(p); }
    }
    found
}

fn main() -> anyhow::Result<()> {
    let args = std::env::args();
    let last = args.last().unwrap();

    if last == "--extract" {
        extract::extract_polygons("planet-240520_admins.pbf", "./extracted/al234.extract_full6")?;
        return Ok(());
    }

    if last == "--filter" {
        filter("./sources/planet-240520.osm.pbf", "./planet-240520.filtered.osm.pbf");
        return Ok(());
    }

    if last == "--eject-via-planet" {
        return eject(
            eject_via_planet::load_planet_extracted_places()?,
            "via-planet_ultrafast_only_ru2.csv",
        );
    }

    Ok(())
}

pub struct RawPlace {
    pub name: String,
    pub name_ru: Option<String>,
    pub al2: Option<String>,
    pub al4: Option<String>,
    pub population: Option<u32>,
    pub lon: f64,
    pub lat: f64,
}
pub fn fetch_places_from_pbf(input_pbf: &str) -> anyhow::Result<Vec<RawPlace>> {
    let pbf_reader = PbfReader::new(&PathBuf::from(input_pbf))?;

    let mut places = Vec::with_capacity(1_000_000);

    'node: for el in pbf_reader.elements()? {
        let node = match el {
            Element::Node { node } => node,
            _ => continue,
        };

        for tag in node.tags().iter() {
            let (tag_key, tag_value) = (tag.k(), tag.v());

            if tag_key != "place" {
                continue;
            }

            match tag_value.as_str() {
                // no village
                "city" | "town" => {
                    let mut al2 = None;
                    let mut al3 = None;
                    let mut al4 = None;
                    let mut name = None;
                    let mut name_ru = None;
                    let mut population = None;

                    for t in node.tags().iter() {
                        if t == tag { continue; }

                        let t_v = t.v().to_owned();
                        match t.k().as_str() {
                            "name" => name = Some(t_v),
                            "name:ru" => name_ru = Some(t_v),
                            "addr:country" => al2 = Some(t_v),
                            "addr:region" => al3 = Some(t_v),
                            "addr:state" => al4 = Some(t_v),
                            "population" => {
                                let mut p = t_v.clone();
                                p.remove_matches(",");
                                p.remove_matches(".");
                                p.remove_matches(" ");
                                population = p.parse().ok();
                            }
                            _ => continue,
                        }
                    }
                    if let Some(name) = name {
                        let c = node.coordinate();
                        places.push(RawPlace {
                            name,
                            name_ru,
                            al2,
                            al4: al4.or(al3),
                            population,
                            lon: c.lon(),
                            lat: c.lat(),
                        });
                    }
                }
                _ => continue,
            }
        }
    }

    Ok(places)
}



fn select_best_parent<'a, AR: AdminRegion + Clone>(parents: &mut Vec<&'a AR>) -> Option<&'a AR> {
    parents.sort_by(
        |a, b|  Ord::cmp(&a.name().len(), &b.name().len())
    );
    parents.sort_by(
        |a, b| Ord::cmp(&b.tags().get(KEY_POPULATION), &a.tags().get(KEY_POPULATION))
    );

    for parent in parents.iter() {
        let tags = parent.tags();
        if [KEY_NAME_RU, KEY_OFFICIAL_NAME_RU].into_iter().any(|k| tags.contains_key(k)){
            return Some(parent);
        }
    }

    parents.first().copied()
}
pub fn eject<AR: AdminRegion + Clone + Sync>(admin_places: Vec<AR>, output: &str) -> anyhow::Result<()> {
    let places = fetch_places_from_pbf("planet-240520.filtered.osm.pbf")?;

    let mut file = BufWriter::new(OpenOptions::new().create(true).append(true).open(output)?);
    file.write("al2,al4_3,place,population,lon,lat\n".as_bytes())?;
    let file = Mutex::new(file);

    let skipped_small = AtomicU64::new(0);
    let no_country = Mutex::new(Vec::with_capacity(1_000_000));
    let no_state = Mutex::new(Vec::with_capacity(10_000));
    let i = AtomicU64::new(0);

    let places_count = places.len();
    println!("{places_count} places will be processed");

    places.par_iter().for_each(|place| {
        let i = i.fetch_add(1, Ordering::Relaxed);

        if place.name_ru.is_none() {
            return;
        }

        // if place.population.is_none() || place.population.is_some_and(|p| p < 10_000) {
        //     skipped_small.fetch_add  (1, Ordering::Relaxed);
        //     return;
        // }

        let mut part_of = HashMap::<u8, Vec<&AR>>::from_iter([
            (2, vec![]),
            // (3, vec![]),
            (4, vec![]),
        ]);

        let place_location = Coord {x: place.lon, y: place.lat};
        for admin in admin_places.iter() {
            if admin.geometry().contains(&place_location) || admin.geometry().intersects(&place_location) {
                part_of
                    .entry(admin.admin_level())
                    .and_modify(|admins| admins.push(admin))
                    .or_insert(vec![admin]);
            }
        }

        let mut final_parents_names = HashMap::<u8, String>::new();

        for (admin_level, mut regions) in part_of.into_iter() {

            let admin_place = select_best_parent(&mut regions);
            let self_defined_admin_name = match admin_level {
                2 => place.al2.as_ref(),
                4 => place.al4.as_ref(),
                _ => None,
            };

            if let Some(admin_place) = admin_place {
                final_parents_names.insert(admin_level, admin_place.try_get_ru_name());
            }
            else if let Some(admin_name) = self_defined_admin_name.cloned() {
                println!(
                    "[{}] parent admin region wasn't found, but place has self-defined value for lvl {}: {}",
                    place.name, admin_level, admin_name
                );

                // searching for admin region by name for ejecting RU name.
                // need somehow skip places that have RU names
                let mut parents_by_name = find_places_by_name(&admin_places, admin_level, &admin_name);
                if parents_by_name.len() > 0 {
                    let s = parents_by_name
                        .iter()
                        .map(|pl| pl.name().clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    println!("found multiple places matching name: {s}")
                }

                let admin = select_best_parent(&mut parents_by_name);
                if let Some(admin) = admin {
                    final_parents_names.insert(admin_level, admin.try_get_ru_name());
                } 
                else {
                    println!("admin place with name:ru was not found for self-defined {} ({})", admin_name, place.name);
                    final_parents_names.insert(admin_level, admin_name);
                    // return;
                }
            }
            else {
                if admin_level == 2 {
                    eprintln!("country wasn't found for place {}, skipping", place.name);
                    no_country.lock().unwrap().push(place);
                    return;
                }
                if admin_level == 4 {
                    let mut message = format!("state wasn't found for {}", place.name);
                    let maybe_parent = place.al2.as_ref().or_else(|| final_parents_names.get(&2));
                    if let Some(parent) = maybe_parent {
                        message = format!("{} ({})", message, parent);
                    }
                    println!("{message}");
                    no_state.lock().unwrap().push(place);
                }
            }
        }

        write!(
            file.lock().unwrap(),
            "{al2},{al4},{place},{population},{lon},{lat}\n",
            al2 = final_parents_names.remove(&2).unwrap(),
            al4 = final_parents_names.remove(&4).unwrap_or_else(|| String::new()),
            place = place.name_ru.as_ref().unwrap_or(&place.name).clone(),
            population = place.population.unwrap_or(0),
            lon = place.lon,
            lat = place.lat,
        );

        if i % 1000 == 0 {
            let percent = (i as f32 / places_count as f32) * 100.0;
            println!("==================PROGRESS {percent:.2}% ({i})==================")
        }
    });

    let mut info_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output.replace(".csv", "-stats.txt"))?;

    let no_country = no_country.lock().unwrap();
    let no_country_list = no_country
        .iter()
        .map(|p| format!("al4={:#?} name={:#?} name_ru={:#?}", p.al4, p.name, p.name_ru.clone().unwrap_or_else(|| String::new())))
        .collect::<Vec<_>>()
        .join("\n");

    let stats = format!(
        "skipped small entries: {}\nskipped_no_country:\n{}",
        skipped_small.load(Ordering::Relaxed),
        no_country_list
    );
    info_file.write(stats.as_bytes())?;
    info_file.flush()?;

    Ok(())
}
