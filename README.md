# CKAN Link Checker

## Introduction

This little hacky script can be used to check whether the links to data on a CKAN data portal are actually working. It has been tested for http://data.overheid.nl/data/api/3 and only works with CKAN version 3 API endpoints. This project was born out of our frustration with the data portal of the Dutch government. We like the idea of data portals, but we found that the links are often broken/outdated. More can be read in our blog post http://openstate.eu/2014/06/nederlands-nauwelijks-nieuwe-datasets-op-data-overheid-nl/ (btw, also check this excellent rant on data portals http://civic.io/2015/04/01/i-hate-open-data-portals/ ).

## Usage example
* download check_ckan_links.py and analyze_results.py (or clone, but then you'll also get all the data.overheid.nl results ;D)
* `./check_ckan_links.py http://data.overheid.nl/data/api/3`

## Result folder content description

* **failed_domain_count.csv**: lists how many times a domain contained a failed resource
* **failed_packages.csv**: list of all packages (i.e. datasets) which failed because none of its resources (i.e. links to data) succeeded
* **failed_resources.csv**: list of failed resoures (i.e. links to data)
* **package_list.json**: json result showing all packages (i.e. datasets) from https://data.overheid.nl/data/api/3/action/package_list
* **packages.csv**: list of all checked packages (i.e. datasets) showing how many of the dataset's resources succeeded with a status code 200 (`ok_resources`), the total number of resources for this dataset (`num_resources)` and the dataset's `id`, `name` and `maintainer`
* **packages_json/**: folder containing all the raw json files returned by the data.overheid.nl API for all packages
* **statistics.txt**: contains two statistics, the number of working data links and the number of packages/datasets without any data links
