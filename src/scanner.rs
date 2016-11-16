use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use regex::Regex;
use std::collections::HashMap;
use std::io::{Stdout, stdout};
use csv::Writer;

pub enum CrunchState {
    Scanning(HashMap<i32,String>, Writer<Stdout>),
    CurrentQuery(Vec<String>, i32, HashMap<i32,String>, Writer<Stdout>)
}

fn hash_query(query_string: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    query_string.hash(&mut hasher);
    hasher.finish()
}

impl CrunchState {
    pub fn new() -> CrunchState {
        let csv_writer = Writer::from_writer(stdout());
        CrunchState::Scanning(HashMap::new(), csv_writer)
    }

    pub fn process_line(self, line:String) -> CrunchState {
        use self::CrunchState::*;
        use self::MatchResult::*;

        match self {
            Scanning(mut pid_to_query, mut csv_writer) => {
                match analyze_line(&line) {
                    Ignore => Scanning(pid_to_query, csv_writer),
                    QueryStart(pid, query_begin) => {
                        let query_parts = vec![query_begin];
                        CurrentQuery(query_parts, pid, pid_to_query, csv_writer)
                    },
                    Duration(pid, duration) => {
                        match pid_to_query.remove(&pid) {
                            Some(full_query) => {
                                let qhash = hash_query(&full_query);
                                let result = csv_writer.encode((pid, duration, qhash, &full_query));
                                assert!(result.is_ok())
                            },
                            None => {
                                // dangling duration
                            }
                        };
                        Scanning(pid_to_query, csv_writer)
                    }
                }
            },
            CurrentQuery(mut query_parts, pid, mut pid_to_query, csv_writer) => {
                if !REGLS.is_match(&line) {
                    query_parts.push(line);
                    CurrentQuery(query_parts, pid, pid_to_query, csv_writer)
                } else {
                    let full_query = query_parts.iter().fold("".to_string(), |acc, s| acc + s);
                    pid_to_query.insert(pid, full_query);
                    let next_state = Scanning(pid_to_query, csv_writer);
                    next_state.process_line(line)
                }
            }
        }
    }
}

lazy_static! {
    static ref REGLS: Regex = Regex::new(r"^2016").unwrap();
    static ref REPID: Regex = Regex::new(r"\d{2,3}\((\d+)\):").unwrap();
    static ref REDURATION: Regex = Regex::new(r"duration: ([0-9.]+) ms").unwrap();
    static ref RESTATEMENT: Regex = Regex::new(r"(?:execute.*|statement):(.*)").unwrap();
}

enum MatchResult {
    Ignore,
    QueryStart(i32, String),
    Duration(i32, String)
}

fn analyze_line(line: &str) -> MatchResult {
    use self::MatchResult::*;

    if REGLS.is_match(&line) {
        match REPID.captures_iter(&line).nth(0) {
            Some(cap) => {
                let pid: &str = cap.at(1).unwrap();
                if REDURATION.is_match(&line) {
                    let duration: &str = REDURATION.captures_iter(&line).nth(0).unwrap().at(1).unwrap();
                    Duration(pid.parse::<i32>().unwrap(), duration.to_string())
                } else if RESTATEMENT.is_match(&line) {
                    let statement: &str = RESTATEMENT.captures_iter(&line).nth(0).unwrap().at(1).unwrap();
                    QueryStart(pid.parse::<i32>().unwrap(), statement.to_string())
                } else {
                    Ignore
                }
            },
            None => {
                Ignore
            }
        }
    } else {
        Ignore
    }
}
