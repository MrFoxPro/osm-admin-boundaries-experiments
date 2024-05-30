#![feature(let_chains)]
#![feature(string_remove_matches)]
#![allow(unused)]

mod eject_via_overpass;
mod eject_via_planet;
mod filter;
mod utils;

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread::{self};

use eject_via_overpass::load_overpass_admin_places;
use eject_via_planet::load_planet_extracted_admin_places;
use filter::filter;
use geo::{Contains, Coord, Intersects, Point};
use osm_io::osm::model::element::Element;

pub trait AdminRegion {
    fn geometry(&self) -> &geo::Geometry<f64>;
    fn name(&self) -> &String;
    fn admin_level(&self) -> u8;
    fn tags(&self) -> &HashMap<String, String>;
}

#[rustfmt::skip]
fn find_places_by_name<'a, T>(places: &'a Vec<T>, lvl: u8, name: &'a String) -> Vec<&'a T> 
where T: AdminRegion
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

    if last == "--filter" {
        filter(
            "./sources/planet-240520.osm.pbf",
            "./planet-240520.filtered.osm.pbf",
        );
        return Ok(());
    }

    if last == "--eject-via-overpass" {
        return eject(load_overpass_admin_places()?, "via-overpass.csv");
    }

    if last == "--eject-via-planet" {
        return eject(
            load_planet_extracted_admin_places()?,
            "via-planet_ultrafast_only_ru.csv",
        );
    }

    Ok(())
}

use osm_io::osm::pbf::reader::Reader as PbfReader;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

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
                        if t == tag {
                            continue;
                        }

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

const POPULATION_KEY: &str = "population";
const NAME_RU_KEY: &str = "name:ru";

#[rustfmt::skip]
fn select_best_parent<AR: AdminRegion>(parents: &mut Vec<&AR>) -> Option<String> {
    parents.sort_by(|a, b| a.name().len().cmp(&b.name().len()));
    parents.sort_by(|a, b| b.tags().get(POPULATION_KEY).cmp(&a.tags().get(POPULATION_KEY)));

    let mut parent_name_ru = Option::<String>::None;

    for r in parents.iter() {
        if r.tags().contains_key(NAME_RU_KEY) {
            parent_name_ru = r.tags().get(NAME_RU_KEY).cloned();
            break;
        }
    }

    if parent_name_ru.is_none() && let Some(reg) = parents.first() {
        let tags = reg.tags();
        let maybe_name_ru = tags.get(NAME_RU_KEY).or(tags.get("official_name:ru"));
        parent_name_ru = Some(maybe_name_ru.cloned().unwrap_or(reg.name().clone()));
    }
    parent_name_ru
}

pub fn eject<AR: AdminRegion + Sync>(admin_places: Vec<AR>, output: &str) -> anyhow::Result<()> {
    let places = fetch_places_from_pbf("planet-240520.filtered.osm.pbf")?;

    // just for make sure
    let admin_places = admin_places
        .into_iter()
        .filter(|ap| (2..=4).contains(&ap.admin_level()))
        .collect::<Vec<_>>();

    let mut file = BufWriter::new(OpenOptions::new().create(true).append(true).open(output)?);
    file.write("al2,al4_3,place,population,lon,lat\n".as_bytes())?;

    let skipped_small = AtomicU64::new(0);
    let no_country = Mutex::new(Vec::with_capacity(1_000_000));
    let no_state = Mutex::new(Vec::with_capacity(10_000));
    let i = AtomicU64::new(0);

    let places_count = places.len();
    println!("{places_count} places will be processed");

    enum WriterCommand {
        Write {
            al2: String,
            al4: String,
            place: String,
            population: u32,
            lon: f64,
            lat: f64,
        },
        Stop,
    }
    let (tx, rx) = std::sync::mpsc::channel::<WriterCommand>();

    let writer_thread = thread::spawn(move || loop {
        while let Ok(v) = rx.recv() {
            match v {
                WriterCommand::Write {
                    al2,
                    al4,
                    place,
                    population,
                    lon,
                    lat,
                } => {
                    let row = format!("{al2},{al4},{place},{population},{lon},{lat}\n",);
                    file.write(row.as_bytes()).unwrap();
                }
                WriterCommand::Stop => {
                    file.flush().unwrap();
                    return;
                }
            }
        }
    });

    places.par_iter().for_each(|place| {
        let i = i.fetch_add(1, Ordering::Relaxed);

        if place.name_ru.is_none() {
            return;
        }

        // if place.population.is_none() || place.population.is_some_and(|p| p < 10_000) {
        //     skipped_small.fetch_add(1, Ordering::Relaxed);
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
            let self_defined_admin_name = match admin_level {
                2 => place.al2.as_ref(),
                4 => place.al4.as_ref(),
                _ => None,
            };

            let admin_name_ru = select_best_parent(&mut regions);

            if let Some(admin_name_ru) = admin_name_ru {
                if let Some(admin_name) = self_defined_admin_name {
                    // println!("self-defined: {admin_name}; selected name ru: {admin_name_ru}");
                }
                if regions.len() > 1 {
                    let s = regions.iter().map(|reg| reg.name().clone()).collect::<Vec<_>>().join(", ");
                    // println!(
                    //     "multiple regions [admin_level={admin_level}] were found for {}: {} (selected {})",
                    //     place.name, s, admin_name_ru
                    // );
                }
                final_parents_names.insert(admin_level, admin_name_ru);
            } 
            else if let Some(admin_name) = self_defined_admin_name {
                println!(
                    "[{}] parent admin region wasn't found, but place has self-defined value for lvl {}: {}",
                    place.name, admin_level, admin_name
                );

                // searching for admin region by name for ejecting RU name.
                // need somehow skip places that have RU names
                let mut parents_by_name = find_places_by_name(&admin_places, admin_level, admin_name);
                if parents_by_name.len() > 0 {
                    let s = parents_by_name
                        .iter()
                        .map(|pl| pl.name().clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    println!("found multiple places matching name: {s}")
                }
                let admin_name_ru = select_best_parent(&mut parents_by_name);
                if let Some(admin_name_ru) = admin_name_ru {
                    final_parents_names.insert(admin_level, admin_name_ru);
                } 
                else {
                    println!("admin place with name:ru was not found for self-defined {} ({})", admin_name, place.name);
                    final_parents_names.insert(admin_level, admin_name.clone());
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
                    println!("state wasn't found for {}, {}", place.al2.clone().or(final_parents_names.get(&2).cloned()).unwrap_or_else(|| String::new()), place.name);
                    no_state.lock().unwrap().push(place);
                }
            }
        }

        tx.send(WriterCommand::Write {
            al2: final_parents_names.remove(&2).unwrap_or("NULL".to_string()),
            al4: final_parents_names.remove(&4).unwrap_or(String::new()),
            place: place.name_ru.as_ref().unwrap_or(&place.name).to_string(),
            population: place.population.unwrap_or(0),
            lon: place.lon,
            lat: place.lat,
        });

        if i % 1000 == 0 {
            let percent = (i as f32 / places_count as f32) * 100.0;
            println!("==================PROGRESS {percent:.2}% ({i})==================")
        }
    });

    tx.send(WriterCommand::Stop);
    writer_thread.join().unwrap();

    let mut statistics_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output.to_owned() + "-stats")?;
    let no_country = no_country.lock().unwrap();
    let no_country_list = no_country
        .iter()
        .map(|p| format!("al4={:#?} name={:#?} name_ru={:#?}", p.al4, p.name, p.name_ru.clone().unwrap_or(String::new())))
        .collect::<Vec<_>>()
        .join("\n");

    let stats = format!(
        "skipped small entries: {}\nskipped_no_country:\n{}",
        skipped_small.load(Ordering::Relaxed),
        no_country_list
    );
    statistics_file.write(stats.as_bytes())?;
    statistics_file.flush()?;

    Ok(())
}
