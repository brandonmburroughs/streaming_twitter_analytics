# Streaming Twitter Analysis

This project is for the purpose of learning more about streaming data systems.  I plan to use Kafka, Spark Streaming, some sort of database (for storing various metrics/aggregations and maybe raw data), and some sort of visual interface (maybe javascript?).  This is a work in progress that I will be updating as I have time.

# Installation

Clone this repo.

```
git clone <url>
```

Install the [kafka-python](https://github.com/mumrah/kafka-python) package.

```
pip install kafka-python
```

Install Spark.  You can find instruction at [District Data Labs](https://districtdatalabs.silvrback.com/getting-started-with-spark-in-python).  You may need to install the `pyspark` python package separately.

Setup Kafka.  I used a [single node VM](https://github.com/chadlung/vagrant-kafka) with Zookeeper and Kafka installed for the sake of simplicity.  You can read more about how to set that up at the provided repo.

# Getting Started

Create a copy of the YAML and update it with your settings (especially the Twitter settings).

```
cp config.yaml.template config.yaml
```

TBD