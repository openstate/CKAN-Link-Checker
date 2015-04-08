#!/usr/bin/env python
# Author: Sicco van Sas (Open State Foundation)

import argparse
import csv
import json
import operator
import os
import sys
import urlparse
from collections import defaultdict


class Analyze():
    def __init__(self, folder):
        self.folder = folder

    def analyze(self):
        if not os.path.exists(self.folder):
            raise ValueError('The specified folder does not exist: %s' % (self.folder))
        
        os.chdir(self.folder)
        
        domains = defaultdict(int)
        
        with open('failed_resources.csv') as IN:
            csv_data = csv.reader(IN)
            for row in csv_data:
                domains[urlparse.urlparse(row[1])[1]] += 1
        
        # Create failed_domain_count.csv, a count of how many times a domain
        # contained a failed resource
        print '\n\nFAILED LINKS PER DOMAIN:'
        with open('failed_domain_count.csv', 'w') as OUT:
            csvwriter = csv.writer(OUT)
            for item in sorted(domains.items(),
                    key=operator.itemgetter(1), reverse=True):
                print '%s: %s' % (item[1], item[0])
                csvwriter.writerow([item[1], item[0]])
        
        
        # Print out and save statistics on the number of working links
        print '\nSTATISTICS:'
        ok_resources = 0
        total_resources = 0
        failed_packages = 0
        total_packages = 0
        with open('packages.csv') as IN:
            csv_data = csv.reader(IN)
            # Skip the column name row
            csv_data.next()
            for row in csv_data:
                ok_resources += int(row[0])
                total_resources += int(row[1])
                if int(row[0]) == 0:
                    failed_packages += 1
                total_packages += 1
        
        failed_resources = total_resources - ok_resources
        
        failed_links = "%s out of %s links failed (%.2f%%)" % (failed_resources , total_resources, (float(failed_resources) / total_resources) * 100)
        failed_packages = "%s out of %s datasets contained no links (%.2f%%)" % (failed_packages , total_packages, (float(failed_packages) / total_packages) * 100)
        print failed_links
        print failed_packages
        
        with open('statistics.txt', 'w') as OUT:
            OUT.write(failed_links + '\n')
            OUT.write(failed_packages + '\n\n')
            OUT.write('Also take a look at failed_domain_count.csv to identify domains with many failed links')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Analyze the results of the CKAN link checker and write the '
        'output to STDOUT and to the files failed_domain_count.csv and '
        'statistics.txt'
    )
    parser.add_argument(
        'results_folder',
        help='folder containing the results of the CKAN link checker',
        nargs=1
    )
    values = parser.parse_args()
    analyze = Analyze(values.results_folder[0])
    analyze.analyze()
