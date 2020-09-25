# Changelog

## 0.1.1

- Break out of sync loop for cursor based pagination if `starting_after` is None
- Pass in `per_page` query param to `next_url`

## 0.1.0

- Add `sla_applied` and `statistics` objects.

## 0.0.4

- Adjust `client.py` to use API `v.2.0`.
- Remove `users` and `leads` streams.
- Add `contacts` stream.
- Add support for `cursor` style paging.

## 0.0.3

- Adjust `client.py` to use API `v.1.4`; fix issues w/ `conversations` and `admins` missinng fields; fix `company_attributes` API version error; fix `write_bookmark` issue when scrolling.

## 0.0.2

- Fix rate limit issue in client.py, intermittently failing in Discover mode.

## 0.0.1

- Initial commit
