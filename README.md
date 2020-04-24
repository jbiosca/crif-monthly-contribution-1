# `target-crif`
Convert a sequence of JSON records into a [CRIF compliant Data File](https://digitalorigin.atlassian.net/wiki/spaces/INT/pages/667779083/CRIF+Italy+Documents?preview=%2F667779083%2F673841179%2FMonthly%20data%20Contribution%20-%20User%20Guide_1%2014.pdf). File is saved to the working directory.

# Formats
Input JSON records must be a sequence of Singer.io messages. The `stream` field will be either `customers` or `loans` depending
on the type of file to be generated.

## `customers` stream
The `customers` stream is a sequence of records to be reported as a _Subject Data File_ from the CRIF protocol.
It is required that the `record` contains all the fields listed in the section _Subject Data Details_ from the Record Layout.
Additionally, the record should include a set of `header__` and `trailer__` fields to be used for the construction of `Header` and `Trailer` records from the Record Layout.

## `loans` stream
// TODO 