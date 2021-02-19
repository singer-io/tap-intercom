# Changelog

## 1.1.3
  * Handle timestamps before 1970-1-1 [#24](https://github.com/singer-io/tap-intercom/pull/24)

## 1.1.2
  * Set replication keys to automatic

## 1.1.1
  * Ensure nested times are transformed

## 1.1.0
  * Query the `contacts` stream as `greater than or equal to` the bookmark [#14](https://github.com/singer-io/tap-intercom/pull/14)

## 1.0.2
  * Support date time strings from API in tranform

## 1.0.1
  * Fix for inconsistem epoch formats from API. Normalize all to milli.

## 1.0.0
  * Bumping to first major version 1.0.0 for GA Release

## 0.1.0
  * Add `sla_applied` and `statistics` objects.

## 0.0.4
  * Adjust `client.py` to use API `v.2.0`.
  * Remove `users` and `leads` streams.
  * Add `contacts` stream.
  * Add support for `cursor` style paging.

## 0.0.3
  * Adjust `client.py` to use API `v.1.4`; fix issues w/ `conversations` and `admins` missinng fields; fix `company_attributes` API version error; fix `write_bookmark` issue when scrolling.

## 0.0.2
  * Fix rate limit issue in client.py, intermittently failing in Discover mode.

## 0.0.1
  * Initial commit
