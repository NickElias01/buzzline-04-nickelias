"""
project_consumer_nickelias.py

Consume json messages from a Kafka topic and visualize author counts in real-time.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict, deque  # data structure for counting author occurrences (defaultdict) and handling message lengths (deque)
from datetime import datetime  # handle timestamps  
import matplotlib.dates as mdates

# Import external packages
from dotenv import load_dotenv
import threading

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("project_json", "project_json")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures
#####################################

# Initialize a dictionary to store author counts
author_counts = defaultdict(int)

# Data structures for new dashboard metrics
category_counts = defaultdict(int)
sentiment_trend = deque(maxlen=20)  # deque for storing the last 5 sentiment scores
message_lengths = []  # list of message lengths

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, axs = plt.subplots(2, 2, figsize=(12, 8))

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################

def plot_author_counts(ax):
    # --- Top-Left: Author Counts Bar Chart ---
    authors_list = list(author_counts.keys())
    counts_list = list(author_counts.values())
    axs[0, 0].bar(authors_list, counts_list, color="skyblue")
    axs[0, 0].set_xlabel("Authors")
    axs[0, 0].set_ylabel("Message Counts")
    axs[0, 0].set_title("Author Message Counts")
    axs[0, 0].set_xticklabels(authors_list)
    fig.autofmt_xdate(bottom=0.2, rotation=45)


def plot_categories(ax):
    # --- Top-Right: Category Distribution Pie Chart ---
    categories = list(category_counts.keys())
    cat_counts = list(category_counts.values())
    if categories:  # Only draw if data exists
        axs[0, 1].pie(cat_counts, labels=categories, autopct="%1.1f%%")
    axs[0, 1].set_title("Message Categories")


def plot_sentiment_trend(ax):
    # --- Bottom-Left: Sentiment Trend Line Chart ---
    if sentiment_trend:
        # Unpack the deque into separate lists
        times = [item[0] for item in sentiment_trend]
        sentiments = [item[1] for item in sentiment_trend]
        mdates_times = mdates.date2num(times)  # Convert datetime objects to numbers
        
        axs[1, 0].plot(mdates_times, sentiments, marker='o', linestyle='-', color="green")
        axs[1, 0].set_xlabel("Timestamp")
        axs[1, 0].set_ylabel("Sentiment")
        axs[1, 0].set_title("Sentiment Trend Over Time")
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))

        # Set x-axis limits (optional, but good practice)
        if times:
            ax.set_xlim(min(mdates_times), max(mdates_times))  # Set x-axis limits

        fig.autofmt_xdate(bottom=0.2, rotation=45)


def plot_message_lengths(ax):
    # --- Bottom-Right: Message Length Histogram ---
    if message_lengths:
        hist_counts, bins, patches = axs[1, 1].hist(message_lengths, bins=10, color="orange", edgecolor="black")  # Get counts
        axs[1, 1].set_xlabel("Message Length")
        axs[1, 1].set_ylabel("Frequency")
        axs[1, 1].set_title("Message Length Distribution")
        if hist_counts.size > 0: # Check if the histogram has any data before finding the max
            axs[1, 1].set_ylim(0, max(hist_counts) * 1.1)  # Set dynamic y-axis limit


def update_dashboard(frame):
    for ax in axs.flatten():
        ax.clear()

    plot_author_counts(axs[0, 0])
    plot_categories(axs[0, 1])
    plot_sentiment_trend(axs[1, 0])
    plot_message_lengths(axs[1, 1])

    fig.tight_layout()  # Move tight_layout here, after all plots are updated

    plt.draw()


#####################################
# Function to process a single message
# #####################################

def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka and update the dashboard data.
    """
    try:
        logger.debug(f"Raw message: {message}")
        message_dict: dict = json.loads(message)
        logger.info(f"Processed JSON message: {message_dict}")

        if isinstance(message_dict, dict):
            # Update author counts
            author = message_dict.get("author", "unknown")
            logger.info(f"Message received from author: {author}")
            author_counts[author] += 1

            # Update category counts
            category = message_dict.get("category", "uncategorized")
            category_counts[category] += 1

            # Update sentiment trend (Corrected and Simplified)
            timestamp_str = message_dict.get("timestamp", "")
            sentiment = message_dict.get("sentiment", 0.0)

            if timestamp_str:  # Only proceed if a timestamp exists
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    sentiment_trend.append((timestamp, sentiment))  # Append only ONCE and with datetime
                except ValueError:
                    logger.error(f"Invalid timestamp format: {timestamp_str}")

            # Update message lengths
            message_length = message_dict.get("message_length", 0)
            message_lengths.append(message_length)

            logger.info(f"Updated metrics - Authors: {dict(author_counts)}, Categories: {dict(category_counts)}")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Set up live visuals
#####################################

def consume_messages(consumer, topic):
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Define main function for this module
#####################################

def main() -> None:
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Start the consumer thread as a daemon thread
    consumer_thread = threading.Thread(target=consume_messages, args=(consumer, topic))
    consumer_thread.daemon = True  # Important: Allow the main thread to exit
    consumer_thread.start()

    # Create the FuncAnimation object to update the dashboard every 5000 ms (5 seconds)
    dashboard_animation = FuncAnimation(fig, update_dashboard, interval=5000)

    # Use a non-blocking pause
    try:
        while True:
            plt.pause(0.1)  # Pause briefly to allow events to be processed
    except KeyboardInterrupt:
        pass  # Handle Ctrl+C gracefully
    
    plt.ioff()  # Turn off interactive mode
    plt.show()  # Keep the final chart displayed

    # Ensure the consumer thread is properly joined
    consumer_thread.join()

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()