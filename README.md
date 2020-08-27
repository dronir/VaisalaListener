
# Vaisala weather station data uploader

This is a tool for connecting to a Vaisala automatic weather station, listening to the
data broadcast, and uploading the data to an InfluxDB database.

## Features

- Listen to the weather station either over serial connection or TCP/IP.
- Upload data to an InfluxDB server.
- Robust: caches data locally if upload fails, and uploads it when things work again.
- Create a mirror of the raw data as a TCP/IP server.

## Requirements

This program uses Python's recent asynchronous tools, and runs on at least Python 3.7
and 3.8.

It requires the following libraries: `toml`, `aiohttp`, `pytest` and `pyserial-asyncio`.

Run `pip install -r requirements.txt` to install the required libraries.

## Basic structure of the program

The program is organized as a pipeline of asynchronous generators which pass the data
down and do various things with it:

- The _listener_ reads the data from the weather station. It can be either a serial
  connection or a TCP/IP connection. If the listener gets data without any errors,
  it yields it onwards and starts reading more.
- The _writer_ gets the data from the listener, and would write the data onto a local drive,
  if turned on in the config. This part is currently not implemented since it was
  not needed, and it simply gets the data from the listener.
- The next step is the _broadcaster_. This essentially creates a server similar to the weather
  station's own TCP/IP broadcast for mirroring, serving the data to connected clients.
- The _parser_ takes the data, which so far is in the format used by the weather station,
  and converts it into the InfluxDB line format.
- The _uploader_ gets data from the _parser_ and uploads it to InfluxDB. It also caches
  the data on local disk if upload fails, and uploads any cached data when it works.

## Usage

Run `python listener.py config.toml` to start the program. Press `ctrl-c` to quit.

The behaviour of the program is determined by the config file. See `test_config.toml`
for an example.

## Testing

To test the system:

- Run an InfluxDB server on your local machine, with a database called `vaisala`.
- Run `python test_server.py`, which will create a TCP/IP server emulating a data source.
- Run the main program with the example config file (`python listener.py test_config.toml`)

This should connect to the test server, receive and process the data and write it to your
InfluxDB database.
