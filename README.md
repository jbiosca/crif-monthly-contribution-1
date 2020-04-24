# `crif-monthly-contribution`
Reads the CRIF monthly contribution from a set of RS tables and prints the table as a [CRIF compliant Data File](https://digitalorigin.atlassian.net/wiki/spaces/INT/pages/667779083/CRIF+Italy+Documents?preview=%2F667779083%2F673841179%2FMonthly%20data%20Contribution%20-%20User%20Guide_1%2014.pdf).

# Formats
Input tables records must be either `customers` or `loans` depending on the type of file to be generated.

## `customers` file
The `customers` file (reported as a _Subject Data File_ from the CRIF protocol) is constructed from a RS table.
It is required that the table contains all the fields listed in the section _Subject Data Details_ from the Record Layout.
Additionally, the record should include a set of `header__` and `trailer__` fields to be used for the construction of `Header` and `Trailer` records from the Record Layout.

```console
$ target_crif.py --table-name crif__customers_monthly_contribution --file-type customers --date 2020-02-02 --dbt-profile dev
```

## `loans` file
// TODO