#!/usr/bin/env python
# Author: Sicco van Sas (Open State Foundation)

import analyze_results

import argparse
import codecs
import cStringIO
import csv
import ftplib
import json
import os
import re
import requests
import socket
import sys
import time
import urllib3
from datetime import datetime
from urlparse import urlparse

# Class to write unicode CSVs (taken from
# https://docs.python.org/2/library/csv.html#examples)
class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

# Append data to a specified CSV
def append_csv(filename, data):
    with open(filename, 'a') as OUT:
        writer = UnicodeWriter(OUT)
        writer.writerow(data)

# Parse arguments
parser = argparse.ArgumentParser(
    description='Takes a CKAN version 3 endpoint as input and checks for each '
    'link in the CKAN data portal if it works (i.e., returns a HTTP status '
    'code 200)'
)
parser.add_argument(
    'endpoint',
    help='URL of the CKAN endpoint, e.g. https://data.overheid.nl/data/api/3',
    nargs=1
)
values = parser.parse_args()

endpoint = values.endpoint[0]

session = requests.session()
session.mount('http://', requests.adapters.HTTPAdapter(max_retries=3))
session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))

# Check whether the specified endpoint is a CKAN version 3 endpoint
if session.get(endpoint).text == '{"version": 3}':
    endpoint = re.sub('/?$', '/action/', endpoint)
else:
    raise ValueError(
        'The supplied URL does not seem to be a CKAN version 3 endpoint'
    )

# Retrieve the package list with all the datasets (i.e. packages)
r = session.get(endpoint + 'package_list')
r.raise_for_status()
rjson = r.json()

# Check if the API call returned successfully
if rjson['success'] == False:
    raise ValueError('%s: %s' % (rjson['__type'], rjson['message']))

dataset_names = rjson['result']

parsed_endpoint = urlparse(endpoint)
# Create folders to save the results in
folder_name = 'results_%s_%s' % (
        parsed_endpoint[1],
        datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    )
if not os.path.exists(folder_name):
    os.mkdir(folder_name)
os.chdir(folder_name)
packages_json_folder = 'packages_json'
os.mkdir(packages_json_folder)

# Save the package list result
with open('package_list.json', 'w') as OUT:
    json.dump(rjson, OUT, indent=4)

# Each checked package will be added to packages.csv listing how
# many of its resources contained ok links, how many resources there
# are in total and the package's ID, name and maintainer
with open('packages.csv', 'w') as ALL_OUT:
    all_writer = UnicodeWriter(ALL_OUT)
    all_writer.writerow([
        'ok_resources',
        'num_resources',
        'id',
        'name',
        'maintainer'
    ])

    # Process each dataset/package
    for dataset_name in dataset_names:
        print '\n' + dataset_name

        # The while loop uses timeout and go as a makeshift mechanism
        # to retry failed requests without hammering the CKAN endpoint
        timeout = 0
        go = True
        rjson = {}
        while go:
            if timeout:
                print 'Sleep ' + str(timeout) + ' seconds'
                time.sleep(timeout)
            try:
                url = endpoint + 'package_show'
                body = {"id": dataset_name}
                r = session.post(
                    url,
                    data=json.dumps(body),
                    headers={'content-type': 'application/json'}
                )
                rjson = r.json()
                go = False
            except (ValueError):
                timeout += 10
                continue

        if timeout > 0:
            timeout -= 1

        # Counts how many resources' links are ok
        ok_resources = 0
        num_resources = 0
        dataset_id = ''
        dataset_maintainer = ''
        # Save the returned JSON result for this dataset
        OUT = open('%s/%s.json' % (packages_json_folder, dataset_name), 'w')
        if 'error' in rjson:
            json.dump(rjson['error'], OUT, indent=4)
        else:
            json.dump(rjson['result'], OUT, indent=4)
            package = rjson['result']
            num_resources = package['num_resources']
            dataset_id = package['id']
            dataset_maintainer = package['maintainer']
            if not dataset_maintainer:
                dataset_maintainer = ''
            # Process each resource (i.e. a link to a data source) of the
            # current dataset/package
            for resource in package['resources']:
                # Sleep at least 0.25 second between requests to avoid
                # hammering the CKAN endpoint too much
                time.sleep(0.25)
                url = resource['url']
                parsed_url = urlparse(url)

                # Parse HTTP URLs
                if parsed_url[0] == 'http':
                    # Try to download the URL and write relevant data to
                    # failed_resources.csv if it fails
                    try:
                        r = session.get(resource['url'], timeout=20)
                    except (socket.timeout,
                            requests.exceptions.Timeout,
                            requests.exceptions.InvalidURL,
                            requests.exceptions.ConnectionError,
                            urllib3.exceptions.LocationParseError) as e:
                        print str(e) + ' : ' + resource['url']
                        append_csv(
                            'failed_resources.csv',
                            [
                                package['name'],
                                resource['url'],
                                '0',
                                str(e)
                            ]
                        )
                        continue

                    # If the HTTP status code is not 200 then save the
                    # results to failed_resources.csv
                    if r.status_code != 200:
                        print 'Got status code %i instead of 200 for: %s' % (
                            r.status_code,
                            resource['url']
                        )
                        append_csv(
                            'failed_resources.csv',
                            [
                                package['name'],
                                resource['url'],
                                str(r.status_code),
                                r.reason
                            ]
                        )
                        continue

                # Parse FTP URLs
                elif parsed_url[0] == 'ftp':
                    # Try to connect to the FTP URL and write relevant data
                    # to failed_resources.csv if it fails
                    try:
                        ftp = ftplib.FTP(parsed_url[1])
                        ftp.login()
                        # Check if we can access the specified path
                        ftp.cwd(''.join(parsed_url[2:]))
                    except ftplib.all_errors as e:
                        print str(e) + ' : ' + resource['url']
                        append_csv(
                            'failed_resources.csv',
                            [
                                package['name'],
                                resource['url'],
                                '0',
                                str(e)
                            ]
                        )
                        continue

                # If we got to this place then the resource's URL is
                # accessible
                ok_resources += 1

        # For each package save the following info
        all_writer.writerow(
            [
                str(ok_resources),
                str(num_resources),
                dataset_id,
                dataset_name,
                dataset_maintainer
            ]
        )
    
        # If the number of ok resources is 0, then output save the
        # package name to failed_packages.csv
        if ok_resources == 0:
            print 'FOUND NO RESOURCES FOR: ' + dataset_name
            with open('failed_packages.csv', 'a') as OUT:
                writer = UnicodeWriter(OUT)
                writer.writerow([dataset_name])

# Perform analysis
os.chdir('..')
analyze = analyze_results.Analyze(folder_name)
analyze.analyze()
