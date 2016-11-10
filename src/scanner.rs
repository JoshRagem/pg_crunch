use std::hash::{Hash, SipHasher, Hasher};
use regex::Regex;
use std::collections::HashMap;
use std::string::String;
use std;
use csv::Writer;

pub enum CrunchState {
    Scanning(HashMap<i32,String>, Writer<std::io::Stdout>),
    CurrentQuery(Vec<String>, i32, HashMap<i32,String>, Writer<std::io::Stdout>)
}

enum MatchResult {
    Ignore,
    QueryStart(i32, String),
    Duration(i32, String)
}

lazy_static! {
    static ref REGLS: Regex = Regex::new(r"^2016").unwrap();
    static ref REPID: Regex = Regex::new(r"\d{2,3}\((\d+)\):").unwrap();
    static ref REDURATION: Regex = Regex::new(r"duration: ([0-9.]+) ms").unwrap();
    static ref RESTATEMENT: Regex = Regex::new(r"(?:execute.*|statement):(.*)").unwrap();
}

pub fn init_state() -> CrunchState {
    let csv_writer: Writer<std::io::Stdout> = Writer::from_writer(std::io::stdout());
    CrunchState::Scanning(HashMap::new(), csv_writer)
}

pub fn process_line(line:String, state:CrunchState) -> CrunchState {
    //println!("line: {}", line);
    match state {
        CrunchState::Scanning(mut pid_to_query, mut csv_writer) => {
            //println!("scanning");
            match analyze_line(line) {
                MatchResult::Ignore => CrunchState::Scanning(pid_to_query, csv_writer),
                MatchResult::QueryStart(pid, query_begin) => {
                    let query_parts = vec![query_begin];
                    CrunchState::CurrentQuery(query_parts, pid, pid_to_query, csv_writer)
                },
                MatchResult::Duration(pid, duration) => {
                    match pid_to_query.remove(&pid) {
                        Some(full_query) => {
                            let mut hasher = SipHasher::new();
                            full_query.hash(&mut hasher);
                            let qhash = hasher.finish();
                            let result = csv_writer.encode((pid, duration, qhash, &full_query));
                            assert!(result.is_ok());
                            //println!("pid={} duration={} query={}", pid, duration, full_query)
                        },
                        None => {
                            //println!("pid={} duration={} dangling duration", pid, duration)
                        }
                    };
                    CrunchState::Scanning(pid_to_query, csv_writer)
                }
            }
        },
        CrunchState::CurrentQuery(mut query_parts, pid, mut pid_to_query, csv_writer) => {
            //println!("query building");
            if !REGLS.is_match(&line) {
                query_parts.push(line);
                CrunchState::CurrentQuery(query_parts, pid, pid_to_query, csv_writer)
            } else {
                let full_query = query_parts.iter().fold("".to_string(), |acc, s| acc + s);
                pid_to_query.insert(pid, full_query);
                process_line(line, CrunchState::Scanning(pid_to_query, csv_writer))
            }
        }
    }
}

fn analyze_line(line:String) -> MatchResult {
    if REGLS.is_match(&line) {
        //println!("good line");
        match REPID.captures_iter(&line).nth(0) {
            Some(cap) => {
                let pid: &str = cap.at(1).unwrap();
                if REDURATION.is_match(&line) {
                    //println!("good duration");
                    let duration: &str = REDURATION.captures_iter(&line).nth(0).unwrap().at(1).unwrap();
                    MatchResult::Duration(pid.parse::<i32>().unwrap(), duration.to_string())
                } else if RESTATEMENT.is_match(&line) {
                    //println!("good statement");
                    let statement: &str = RESTATEMENT.captures_iter(&line).nth(0).unwrap().at(1).unwrap();
                    MatchResult::QueryStart(pid.parse::<i32>().unwrap(), statement.to_string())
                } else {
                    //println!("ignore?");
                    MatchResult::Ignore
                }
            },
            None => {
                //println!("no pid");
                MatchResult::Ignore
            }
        }
    } else {
        //println!("good ignore");
        MatchResult::Ignore
    }
}
