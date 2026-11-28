# Cordata

Cordata maintains a Bluesky moderation list of accounts that liked or reposted each configured post. Each account appears once, including the list owner or someone they follow if they qualify. Undoing a like leaves the account on the list while its repost remains, and vice versa. Quote posts alone do not qualify.

## Run

Set `CORDATA_IDENTIFIER` and `CORDATA_APP_PASSWORD` for the list owner. Run Cordata from the repository root.

```sh
nix run .#cordata -- 'https://bsky.app/profile/bsky.app/post/3mzyk34ho4c27'
```

Pass one or more Bluesky post links or AT URIs as separate arguments. Each distinct post gets its own list and retry schedule. All posts share the owner’s session and write interval. Cordata resolves a handle to a DID before choosing the list. The list has a title derived from its AT URI and a description that contains the post's DID-based AT URI. When Cordata renames a list whose old title was a handle-based post URI, it keeps that URI in the description.

The `maintaining list` log entry shows the list's AT URI. Subscribe to the list in Bluesky to use its moderation actions. Run `nix run .#cordata -- --help` for all options. Flags override the corresponding `CORDATA_` environment variables, which use uppercase names and underscores in place of hyphens.

## Membership updates

Cordata combines AppView’s lists of likes and reposts with Constellation backlinks for both record types. For accounts found only through Constellation, it checks the corresponding record on the actor’s PDS. Up to four actors can be checked concurrently across posts, with at most two checks per PDS. If an actor’s PDS is unreachable, Cordata preserves that account’s existing membership and retries verification. Other accounts can still be added or removed. If AppView or Constellation fails to return a complete set of pages, Cordata can add confirmed accounts but defers removals for that list. Posts ready before the owner’s list-item scan starts share that scan. Each post uses only its own items. An incomplete inventory defers reconciliation for every post sharing it. Set `--constellation` to use a different backlink endpoint. The owner's repository stores the list and its items across restarts.

Cordata waits five minutes after a successful pass by default. Failures retry after one minute, doubling on consecutive failures up to the normal scan interval (with a one-minute floor and a one-hour ceiling). Set `--rescan-interval` to change the delay after successful passes. Set `--write-interval` to change the minimum interval between writes, which defaults to ten seconds.

## Deploy to Kubernetes

Create a Secret named `cordata` with an `app-password` key in the release namespace. Install the chart from the repository root.

```sh
helm upgrade --install cordata ./cordata/charts/cordata \
	--set identifier=you.example.com --set existingSecret=cordata \
	--set-string post='at://bsky.app/app.bsky.feed.post/3mzyk34ho4c27'
```

For multiple posts, set the Helm value `posts` to a sequence of links or AT URIs. The single `post` value remains supported. Migrating under the same owner preserves each post’s list URI and subscriptions. Removing a post stops updates to its list.

The [chart values](charts/cordata/values.yaml) cover endpoints and scan and write intervals. The chart runs one replica using the image from `ghcr.io/alyraffauf/terrarium/cordata`. Set `image.tag` to a published `sha-<commit>` tag to pin a release.

## Metrics

Set `--metrics-address=:9090` to expose Prometheus metrics at `/metrics`. The listener is disabled by default. `terrarium_verification_deferred_total` counts failed like or repost record verification attempts scheduled for retry. `terrarium_verification_recovered_total` counts successful retries. `terrarium_members_added_total` and `terrarium_members_removed_total` count successful list-item writes across all managed lists. All four counters reset on restart.
