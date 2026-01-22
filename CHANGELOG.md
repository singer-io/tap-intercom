# Changelog

## 2.2.4
  * Fix interrupted bookmarking for conversations [#85](https://github.com/singer-io/tap-intercom/pull/85)

## 2.2.3
  * Fix pylint errors [#83](https://github.com/singer-io/tap-intercom/pull/83)

## 2.2.2
  * Bump dependency versions for twistlock compliance [#80](https://github.com/singer-io/tap-intercom/pull/80)

## 2.2.1
 * Improve companies stream completion [#77](https://github.com/singer-io/tap-intercom/pull/77)

## 2.2.0
 * API version upgrade to 2.12 [#76](https://github.com/singer-io/tap-intercom/pull/76)

## 2.1.1
 * Bump requests from 2.23.0 to 2.32.3 [#62](https://github.com/singer-io/tap-intercom/pull/62)

## 2.1.0
 * Fix conversations stream bookmarking [#73](https://github.com/singer-io/tap-intercom/pull/73)
 * Update conversations schema [#71](https://github.com/singer-io/tap-intercom/pull/71)
 * Update contacts schema [#70](https://github.com/singer-io/tap-intercom/pull/70)

## 2.0.2
 * Retry on responses with no JSON [#66](https://github.com/singer-io/tap-intercom/pull/66)

## 2.0.1
 * Fix schema for Conversations custom_attributes field [#63](https://github.com/singer-io/tap-intercom/pull/63)

## 2.0.0
 * Update primary keys for company_attributes and contacts stream [#56](https://github.com/singer-io/tap-intercom/pull/56)
 * Update API version and related new fields [#53](https://github.com/singer-io/tap-intercom/pull/53)
 * Use common API calls for parent's data for parent/child stream [#55](https://github.com/singer-io/tap-intercom/pull/55)
 * Add addressable list fields [#58](https://github.com/singer-io/tap-intercom/pull/58)
 * Add custom exception handling [#54](https://github.com/singer-io/tap-intercom/pull/54)
 * Add missing tap tester and unit tests [#57](https://github.com/singer-io/tap-intercom/pull/57)

## 1 1.7
 * Change company stream from Full to Incremental [#44](https://github.com/singer-io/tap-intercom/pull/44)
 * Fix bookmark for conversation_parts stream [#45](https://github.com/singer-io/tap-intercom/pull/45)
 * Make logger message more descriptive [#46](https://github.com/singer-io/tap-intercom/pull/46)
 * Added missing fields for contact stream [#47](https://github.com/singer-io/tap-intercom/pull/47)
## 1.1.6
 * Fix OOM issue [#40] (https://github.com/singer-io/tap-intercom/pull/40)

## 1.1.5
 * Fix 'str' has no attribute get for getting bookmark. This is a fix for the previous version 1.1.4 which was rolled back  [#38] (https://github.com/singer-io/tap-intercom/pull/38)

## 1.1.4
  * Request Timeout Implementation [#36](https://github.com/singer-io/tap-intercom/pull/36)

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
