extern crate goal_consumer;
extern crate mongodb;

use goal_consumer::GoalConsumer;
use mongodb::{Client, ThreadedClient};

fn main() {
  let goal_consumer = GoalConsumer {
    mongo_client: Client::connect("localhost", 27017).expect("Couldn't connect to mongodb"),
    addr: "127.0.0.1:5672",
    queue_name: "***",
    consumer_tag: "***",
    collection_name: "***",
    db_name: "***",
  };

  // goal_consumer.playground();
  // goal_consumer.get_events();
  goal_consumer.start_consuming();
}
