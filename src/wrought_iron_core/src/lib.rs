use pyo3::prelude::*;
use rayon::prelude::*;
use strsim::levenshtein;
use regex::Regex;
use sha2::{Sha256, Digest};
use std::collections::HashMap;
use serde_json::Value;
use rstar::{RTree, PointDistance, RTreeObject, AABB};

// --- GEO MODULE ---

#[derive(Clone, Copy, Debug, PartialEq)]
struct Point {
    id: usize,
    x: f64, // lon
    y: f64, // lat
}

impl RTreeObject for Point {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.x, self.y])
    }
}

impl PointDistance for Point {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.x - point[0];
        let dy = self.y - point[1];
        dx * dx + dy * dy
    }
}

#[pyfunction]
fn haversine_batch(lats: Vec<f64>, lons: Vec<f64>, target_lat: f64, target_lon: f64) -> Vec<f64> {        
    const EARTH_RADIUS_KM: f64 = 6371.0;
    let target_lat_rad = target_lat.to_radians();
    let target_lon_rad = target_lon.to_radians();

    lats.par_iter()
        .zip(lons.par_iter())
        .map(|(&lat, &lon)| {
            let lat_rad = lat.to_radians();
            let lon_rad = lon.to_radians();
            let dlat = lat_rad - target_lat_rad;
            let dlon = lon_rad - target_lon_rad;
            let a = (dlat / 2.0).sin().powi(2) +
                    lat_rad.cos() * target_lat_rad.cos() * (dlon / 2.0).sin().powi(2);
            let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
            EARTH_RADIUS_KM * c
        })
        .collect()
}

#[pyfunction]
fn nearest_neighbor_batch(
    source_lats: Vec<f64>,
    source_lons: Vec<f64>,
    target_ids: Vec<String>,
    target_lats: Vec<f64>,
    target_lons: Vec<f64>
) -> (Vec<String>, Vec<f64>) {
    let points: Vec<Point> = target_lats.iter().zip(target_lons.iter()).enumerate()
        .map(|(i, (&lat, &lon))| Point { id: i, x: lon, y: lat })
        .collect();
    let rtree = RTree::bulk_load(points);

    let results: Vec<(String, f64)> = source_lats.par_iter().zip(source_lons.par_iter())
        .map(|(&lat, &lon)| {
            match rtree.nearest_neighbor(&[lon, lat]) {
                Some(p) => {
                    let d = haversine_batch(vec![lat], vec![lon], p.y, p.x)[0];
                    (target_ids[p.id].clone(), d)
                },
                None => ("".to_string(), -1.0)
            }
        })
        .collect();

    let (ids, dists): (Vec<String>, Vec<f64>) = results.into_iter().unzip();
    (ids, dists)
}

#[pyfunction]
fn validate_coords_batch(lats: Vec<f64>, lons: Vec<f64>) -> Vec<bool> {
    lats.par_iter().zip(lons.par_iter())
        .map(|(&lat, &lon)| lat >= -90.0 && lat <= 90.0 && lon >= -180.0 && lon <= 180.0)
        .collect()
}

// --- AUDIT MODULE ---

#[pyfunction]
fn scan_pii_batch(texts: Vec<String>) -> Vec<Vec<String>> {
    let email_re = Regex::new(r"(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}").unwrap();
    let cc_re = Regex::new(r"\b(?:\d[ -]*?){13,16}\b").unwrap();
    let phone_re = Regex::new(r"\b(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})\b").unwrap();

    texts.par_iter().map(|text| {
        let mut findings = Vec::new();
        if email_re.is_match(text) { findings.push("EMAIL".to_string()); }
        if cc_re.is_match(text) { findings.push("CREDIT_CARD".to_string()); }
        if phone_re.is_match(text) { findings.push("PHONE".to_string()); }
        findings
    }).collect()
}

#[pyfunction]
fn hash_batch(texts: Vec<String>, salt: String) -> Vec<String> {
    texts.par_iter().map(|text| {
        let mut hasher = Sha256::new();
        hasher.update(salt.as_bytes());
        hasher.update(text.as_bytes());
        let result = hasher.finalize();
        hex::encode(result)
    }).collect()
}

#[pyfunction]
fn mask_batch(texts: Vec<String>, chars: usize) -> Vec<String> {
    texts.par_iter().map(|text| {
        if text.len() <= chars {
            "*".repeat(text.len())
        } else {
            format!("{}{}", "*".repeat(chars), &text[chars..])
        }
    }).collect()
}

// --- CLEAN MODULE ---

#[pyfunction]
fn find_duplicates(values: Vec<String>, threshold: f64) -> Vec<(usize, String)> {
    values.par_iter().enumerate().map(|(i, val)| {
        let mut best_match: Option<String> = None;
        let mut best_score = 0.0;
        for other in &values {
            if val == other { continue; }
            let dist = levenshtein(val, other);
            let max_len = val.len().max(other.len());
            let ratio = if max_len == 0 { 1.0 } else { 1.0 - (dist as f64 / max_len as f64) };
            let score = ratio * 100.0;
            if score > best_score && score >= threshold {
                best_score = score;
                best_match = Some(other.clone());
            }
        }
        if let Some(m) = best_match { (i, m) } else { (i, val.clone()) }
    }).collect()
}

#[pyfunction]
fn harmonize_batch(values: Vec<String>, threshold: f64) -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    let mut processed = std::collections::HashSet::new();
    for val in &values {
        if processed.contains(val) { continue; }
        let matches: Vec<String> = values.iter()
            .filter(|&other| {
                if processed.contains(other) { return false; }
                let dist = levenshtein(val, other);
                let max_len = val.len().max(other.len());
                let ratio = if max_len == 0 { 1.0 } else { 1.0 - (dist as f64 / max_len as f64) };
                ratio * 100.0 >= threshold
            })
            .cloned()
            .collect();
        for m in matches { mapping.insert(m.clone(), val.clone()); processed.insert(m); }
    }
    mapping
}

#[pyfunction]
fn detect_outliers_zscore(data: Vec<f64>, threshold: f64) -> Vec<bool> {
    if data.is_empty() { return vec![]; }
    let n = data.len() as f64;
    let mean: f64 = data.iter().sum::<f64>() / n;
    let variance: f64 = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    let std_dev = variance.sqrt();
    data.par_iter().map(|&x| {
        if std_dev == 0.0 { false } else { (x - mean).abs() / std_dev > threshold }
    }).collect()
}

#[pyfunction]
fn regex_replace_batch(texts: Vec<String>, pattern: String, repl: String) -> Vec<String> {
    let re = Regex::new(&pattern).unwrap();
    texts.par_iter().map(|text| re.replace_all(text, repl.as_str()).to_string()).collect()
}

#[pyfunction]
fn validate_schema_batch(texts: Vec<String>, expected_type: String) -> Vec<bool> {
    texts.par_iter().map(|text| {
        match expected_type.as_str() {
            "int" => text.parse::<i64>().is_ok(),
            "float" => text.parse::<f64>().is_ok(),
            "json" => serde_json::from_str::<Value>(text).is_ok(),
            _ => true,
        }
    }).collect()
}

// --- SCHEMA MODULE ---

#[pyfunction]
fn detect_json_batch(texts: Vec<String>) -> Vec<bool> {
    texts.par_iter().map(|text| serde_json::from_str::<Value>(text).is_ok()).collect()
}

// --- QUERY MODULE ---

#[pyfunction]
fn search_batch(texts: Vec<String>, term: String, case_sensitive: bool) -> Vec<bool> {
    let term_lower = if !case_sensitive { term.to_lowercase() } else { term.clone() };
    texts.par_iter().map(|text| {
        if case_sensitive { text.contains(&term) } else { text.to_lowercase().contains(&term_lower) }
    }).collect()
}

// --- AGGREGATE MODULE ---

#[pyfunction]
fn calculate_stats_batch(data: Vec<f64>) -> HashMap<String, f64> {
    let mut stats = HashMap::new();
    if data.is_empty() { return stats; }
    let n = data.len() as f64;
    let mean = data.iter().sum::<f64>() / n;
    let variance = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    let std_dev = variance.sqrt();
    let mut m3 = 0.0; let mut m4 = 0.0;
    for &x in &data { let d = x - mean; m3 += d.powi(3); m4 += d.powi(4); }
    m3 /= n; m4 /= n;
    stats.insert("skew".to_string(), if std_dev > 0.0 { m3 / std_dev.powi(3) } else { 0.0 });
    stats.insert("kurtosis".to_string(), if variance > 0.0 { m4 / variance.powi(2) - 3.0 } else { 0.0 });
    stats
}

// --- MODULE DEFINITION ---

#[pymodule]
fn wrought_iron_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(haversine_batch, m)?)?;
    m.add_function(wrap_pyfunction!(nearest_neighbor_batch, m)?)?;
    m.add_function(wrap_pyfunction!(validate_coords_batch, m)?)?;
    m.add_function(wrap_pyfunction!(scan_pii_batch, m)?)?;
    m.add_function(wrap_pyfunction!(hash_batch, m)?)?;
    m.add_function(wrap_pyfunction!(mask_batch, m)?)?;
    m.add_function(wrap_pyfunction!(find_duplicates, m)?)?;
    m.add_function(wrap_pyfunction!(harmonize_batch, m)?)?;
    m.add_function(wrap_pyfunction!(detect_outliers_zscore, m)?)?;
    m.add_function(wrap_pyfunction!(regex_replace_batch, m)?)?;
    m.add_function(wrap_pyfunction!(validate_schema_batch, m)?)?;
    m.add_function(wrap_pyfunction!(detect_json_batch, m)?)?;
    m.add_function(wrap_pyfunction!(search_batch, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_stats_batch, m)?)?;
    Ok(())
}
