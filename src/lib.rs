#[macro_use(doc, bson)]
extern crate bson;
extern crate chrono;
extern crate futures;
extern crate mongodb;
extern crate time;

#[macro_use(json)]
extern crate serde_json;
extern crate tokio;

extern crate lapin_futures;

use bson::Bson::Document as BsonDocument;
use bson::{Bson, Document, EncoderError, UtcDateTime};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::future::Future;
use futures::Stream;
use lapin_futures::channel::{BasicConsumeOptions, QueueDeclareOptions};
use lapin_futures::client::ConnectionOptions;
use lapin_futures::types::FieldTable;
use mongodb::db::ThreadedDatabase;
use mongodb::{Client, ThreadedClient};
use serde_json::Value;
use std::format;
use time::Duration;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;

pub struct GoalConsumer {
    pub mongo_client: Client,
    pub addr: &'static str,
    pub queue_name: &'static str,
    pub consumer_tag: &'static str,
    pub collection_name: &'static str,
    pub db_name: &'static str,
}

impl GoalConsumer {
    pub fn playground(&self) {
        println!("This is the playground function. Use this to learn things about Rust!");
        // let now_ts = Utc::now() + Duration::minutes(30);
        // let now = DateTime::naive_utc(&now_ts);
        // let now_updated = DateTime::<Utc>::from_utc(now, Utc);
        // println!("{}", now_updated);

        // let mut check = None;

        // let doc = doc! {
        //     "test": {
        //         "another_test": "blue"
        //     }
        // };

        // check = Some("hello");

        // if let Some(_yep) = check {
        //     println!("yep");
        // }

        // match doc.get("test") {
        //     Some(d) => match &d {
        //         BsonDocument(r) => {
        //             let mut x = r.clone();
        //             x.insert("added_test", "green");
        //             println!("{:?}", x);
        //         }
        //         _ => (),
        //     },
        //     _ => (),
        // }
        // println!("doc: {:?}", doc);
    }

    pub fn get_events(&self) {
        let date_string = "2018-10-25 00:00:00.000";
        let dt = NaiveDateTime::parse_from_str(&date_string, "%Y-%m-%d %H:%M:%S%.3f").unwrap();
        let udt = DateTime::<Utc>::from_utc(dt, Utc);

        let goals = self
            .mongo_client
            .db(self.db_name)
            .collection(self.collection_name);

        let initial_query = doc! {
            "entity_id": "***",
            "start_date": {
                "$lt": udt,
            },
            "end_date": {
                "$gt": udt,
            },
            format!("calc_vars.{}", "sale"): {
                "$exists": true,
            }
        };

        let cursor = goals
            .find(Some(initial_query.clone()), None)
            .ok()
            .expect("Failed to execute find");

        for item in cursor {
            if let Ok(record) = item {
                let x = record.get("calc_vars").unwrap().clone();
                let y = x.as_document().unwrap().get("sale").unwrap();
                let z = y.as_array().unwrap();

                let mut w: Vec<i32> = vec![];

                for item in z.iter() {
                    w.push(item.as_i32().unwrap());
                }
                // w.push(-1);
                w.pop();

                let update_filter = doc! {
                    "_id": record.get("_id").unwrap().clone()
                };

                let update_query = doc! {
                    "$set": {
                        format!("calc_vars.{}", "sale"): bson::to_bson(&w).unwrap()
                    }
                };

                println!("update_filter: {:?}", update_filter);
                println!("update_query: {:?}", update_query);

                match goals.update_one(update_filter, update_query, None) {
                    Ok(_r) => println!("Doc updated!"),
                    _ => println!("Failed to update doc"),
                };
            }
        }
    }

    pub fn start_consuming<'a>(&self) {
        let addr = self.addr.parse().unwrap();
        Runtime::new()
            .unwrap()
            .block_on_all(
                TcpStream::connect(&addr)
                    .and_then(|stream: TcpStream| {
                        lapin_futures::client::Client::connect(stream, ConnectionOptions::default())
                    })
                    .and_then(|(client, heartbeat)| {
                        tokio::spawn(heartbeat.map_err(|_| ()));
                        client.create_channel()
                    })
                    .and_then(|channel| {
                        let _id = channel.id;
                        let ch = channel.clone();
                        channel
                            .queue_declare(
                                "goals",
                                QueueDeclareOptions::default(),
                                FieldTable::new(),
                            )
                            .and_then(move |queue| {
                                channel.basic_consume(
                                    &queue,
                                    "rust-goal-consumer",
                                    BasicConsumeOptions::default(),
                                    FieldTable::new(),
                                )
                            })
                            .and_then(|stream| {
                                /*
                                    This is where all the Goal & MongoDB magic happens
                                */

                                let client = Client::connect("localhost", 27017)
                                    .expect("Couldn't connect to mongodb");
                                let parse_from_str = NaiveDateTime::parse_from_str;

                                stream.for_each(move |message| {
                                    // Convert message to JSON
                                    let event = std::str::from_utf8(&message.data).unwrap();
                                    let event: Value = match serde_json::from_str(event) {
                                        Ok(j) => j,
                                        Err(e) => panic!("{:?}", e),
                                    };

                                    // Extract queried values
                                    let entity_id = event["entity_id"].as_str().unwrap();
                                    let event_type = event["event_type"].as_str().unwrap();
                                    let event_ts = event["ts"].as_str().unwrap();


                                    // Alert received event
                                    println!(
                                        "Received event => {} -- {} -- {}",
                                        event_ts, event_type, entity_id
                                    );

                                    if event_type == "sale" || event_type == "rmr" {
                                        let event_dt =
                                        parse_from_str(event_ts, "%Y-%m-%d %H:%M:%S%.3f").unwrap();
                                    let event_dt_formatted =
                                        DateTime::<Utc>::from_utc(event_dt, Utc);

                                    // Format query
                                    let current_goals_for_player_by_event_type = doc! {
                                        "entity_id": entity_id,
                                        "start_date": {
                                            "$lt": event_dt_formatted,
                                        },
                                        "end_date": {
                                            "$gt": event_dt_formatted,
                                        },
                                        format!("calc_vars.{}", event_type): {
                                            "$exists": true,
                                        },
                                    };

                                    // Query Mongo
                                    let goals = client
                                        .db("***")
                                        .collection("***")
                                        .find(Some(current_goals_for_player_by_event_type), None)
                                        .ok()
                                        .expect("Failed to execute query.");

                                    // // Iterate through results if present
                                    for item in goals {
                                        if let Ok(goal) = item {
                                            println!("----------------");
                                            println!("-- Valid goal found for event: {:?}", event);
                                            println!("-- goal: {:?}", goal);

                                            let calc_vars = goal
                                                .get("calc_vars")
                                                .unwrap()
                                                .clone();
                                            let event_type_doc = calc_vars
                                                .as_document()
                                                .unwrap()
                                                .get(&event_type)
                                                .unwrap();
                                            let event_type_arr = event_type_doc
                                                .as_array()
                                                .unwrap();

                                            let value: i32 = event["value"].as_str().unwrap().parse().unwrap();

                                            let mut updated_calc_vars: Vec<i32> = vec![];

                                            for item in event_type_arr.iter() {
                                                if let Some(i) = item.as_i32() {
                                                    updated_calc_vars.push(i);
                                                } else {
                                                    if let Some(i) = item.as_str() {
                                                        updated_calc_vars.push(i.parse().unwrap());
                                                    }
                                                }
                                            }
                                            updated_calc_vars.push(value);
                                            let updated_calc_vars_bson = bson::to_bson(&updated_calc_vars).unwrap();
                                            let mut progress: i32 = updated_calc_vars.iter().sum();
                                            if progress < 0 {
                                                progress = 0;
                                            }

                                            let current_status = goal.get("status").unwrap().as_str().unwrap();
                                            let mut updated_status = current_status;
                                            let mut target_i64: i32 = 0;
                                            if let bson::Bson::I32(target) = goal.get("target").unwrap() {
                                                target_i64 = *target as i32;
                                            };
                                            let mut updated_auto_accept_ts = None;

                                            if current_status == "pending" {
                                                updated_status = "undecided"
                                            }

                                            if progress >= target_i64 {
                                                updated_status = "hit"
                                            }

                                            if current_status == "pending" && updated_status == "undecided" {
                                                let forward_ts = Utc::now() + Duration::minutes(30);
                                                let forward_ts_utc = DateTime::naive_utc(&forward_ts);
                                                updated_auto_accept_ts = Some(DateTime::<Utc>::from_utc(forward_ts_utc, Utc));
                                            }

                                            let update_filter = doc! {
                                                "_id": goal.get("_id").unwrap().clone()
                                            };

                                            let mut update_query = doc! {
                                                "$set": {
                                                    format!("calc_vars.{}", event_type): updated_calc_vars_bson,
                                                    "progress": progress,
                                                    "status": updated_status,
                                                }
                                            };

                                            if let Some(auto_accept) = updated_auto_accept_ts {
                                                match update_query.get("$set") {
                                                    Some(u) => match &u {
                                                        BsonDocument(s) => {
                                                            let mut new_set = s.clone();
                                                            new_set.insert("auto_accept_ts", auto_accept);
                                                            update_query.insert("$set", new_set);
                                                            println!("-- auto accept query: {:?}", update_query);
                                                        },
                                                        _ => ()
                                                    },
                                                    _ => ()
                                                }
                                            }

                                            // Update Mongo
                                            match client.db("***").collection("***").update_one(update_filter, update_query, None) {
                                                Ok(_r) => println!("-- Goal updated --"),
                                                _ => println!("!! Failed to update goal !!")
                                            }

                                            println!("----------------");
                                        }
                                    }
                                    }



                                    ch.basic_ack(message.delivery_tag, false)
                                })
                            })
                    }),
            )
            .expect("runtime exited with error");
    }
}
