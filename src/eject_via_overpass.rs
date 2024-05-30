use std::{collections::HashMap, str::FromStr};

use geo::Geometry;
use geojson::de::{deserialize_feature_collection_str_to_vec, deserialize_geometry};
use serde::{Deserialize, Deserializer};

use crate::{eject, AdminRegion};

fn deserialize_u32<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    u32::from_str(&buf).map_err(serde::de::Error::custom)
}

fn deserialize_u8<'de, D>(deserializer: D) -> Result<u8, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    u8::from_str(&buf).map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParsedTags {
    #[serde(default = "String::new")]
    name: String,

    #[serde(default, deserialize_with = "deserialize_u8")]
    admin_level: u8,

    #[serde(default, deserialize_with = "deserialize_u32")]
    population: u32,

    #[serde(flatten)]
    extra: HashMap<String, String>,
}
#[derive(Debug, Clone, Deserialize)]
pub struct ConvertedAdminPlace {
    pub id: serde_json::Value,
    #[serde(deserialize_with = "deserialize_geometry")]
    pub geometry: Geometry<f64>,
    pub tags: ParsedTags,
}

impl AdminRegion for ConvertedAdminPlace {
    fn geometry(&self) -> &Geometry<f64> {
        &self.geometry
    }

    fn name(&self) -> &String {
        &self.tags.name
    }

    fn admin_level(&self) -> u8 {
        self.tags.admin_level
    }

    fn tags(&self) -> &HashMap<String, String> {
        &self.tags.extra
    }
}

pub fn load_overpass_admin_places() -> anyhow::Result<Vec<ConvertedAdminPlace>> {
    let mut values = ["./overpass/al2.geom.geojson", "./overpass/al4.geom.geojson"]
        .map(std::fs::read_to_string)
        .map(Result::unwrap)
        .map(|s| deserialize_feature_collection_str_to_vec::<ConvertedAdminPlace>(&s))
        .map(Result::unwrap)
        .concat()
        .into_iter()
        .filter(|item| {
            if item.tags.name.is_empty() {
                eprintln!("admin {:#?} doesn't have name!", item.id)
            }
            !item.tags.name.is_empty()
        })
        .collect::<Vec<_>>();

    for v in values.iter_mut() {
        v.tags.extra.extend(
            [
                ("name".to_owned(), v.tags.name.to_string()),
                ("admin_level".to_owned(), v.tags.admin_level.to_string()),
                ("population".to_owned(), v.tags.population.to_string()),
            ]
            .into_iter(),
        );
    }

    Ok(values)
}