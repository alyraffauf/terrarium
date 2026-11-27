# Cordata

Cordata maintains a Bluesky moderation list of accounts that liked one post. Set `CORDATA_IDENTIFIER` and `CORDATA_APP_PASSWORD` for the list owner, then run:

```sh
nix run .#cordata -- 'https://bsky.app/profile/bsky.app/post/3mzyk34ho4c27'
```

The target can also be an AT URI. The description contains the post’s DID-based AT URI. When the existing description matches the target and the list title is a handle-based URI for the same post, Cordata preserves the old title in the description.

Cordata includes the list owner and accounts they follow if they liked the post. It scans public AppView likes and the owner's list items, then adds or removes members. It waits five minutes after each successful pass and one minute after a failed pass and spaces writes ten seconds apart. Use `--rescan-interval` and `--write-interval` to adjust those timings. The owner's repo stores the list and its members across restarts.

## Kubernetes

Create a Secret containing the owner's app password under the key `app-password`. Install the chart in the same namespace:

```sh
helm upgrade --install cordata ./cordata/charts/cordata \
  --set identifier=you.example.com --set existingSecret=cordata \
  --set-string post='at://bsky.app/app.bsky.feed.post/3mzyk34ho4c27'
```

The chart runs one replica using the image from `ghcr.io/alyraffauf/terrarium/cordata`. Set `image.tag` to a published `sha-<commit>` tag to pin a release.
