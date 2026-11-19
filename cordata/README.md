# Cordata

Cordata maintains a Bluesky moderation list of accounts that liked one post. Set `CORDATA_IDENTIFIER` and `CORDATA_APP_PASSWORD` for the list owner, then run:

```sh
nix run .#cordata -- 'https://bsky.app/profile/bsky.app/post/3mzyk34ho4c27'
```

The target can also be an AT URI. The list title is the post's DID-based AT URI when it fits Bluesky's 64-byte title limit. Otherwise Cordata uses the handle-based AT URI, such as `at://bsky.app/app.bsky.feed.post/3mzyk34ho4c27`. If neither fits, startup fails. The description contains the full DID-based URI.

Cordata scans public AppView likes and the owner's list items, then adds or removes members. It waits five minutes after each completed pass and spaces writes ten seconds apart. Use `--rescan-interval` and `--write-interval` to adjust those timings. The owner's repo stores the list and its members across restarts.

## Kubernetes

Create a Secret containing the owner's app password under the key `app-password`. Install the chart in the same namespace:

```sh
helm upgrade --install cordata ./cordata/charts/cordata \
  --set identifier=you.example.com --set existingSecret=cordata \
  --set-string post='at://bsky.app/app.bsky.feed.post/3mzyk34ho4c27'
```

The chart runs one replica using the image from `ghcr.io/alyraffauf/terrarium/cordata`. Set `image.tag` to a published `sha-<commit>` tag to pin a release.
