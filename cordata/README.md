# Cordata

Cordata maintains a Bluesky moderation list of accounts that liked each configured post. The list includes the owner and any accounts the owner follows that liked the post.

## Run

Set `CORDATA_IDENTIFIER` and `CORDATA_APP_PASSWORD` for the list owner. Run Cordata from the repository root.

```sh
nix run .#cordata -- 'https://bsky.app/profile/bsky.app/post/3mzyk34ho4c27'
```

Pass one or more Bluesky post links or AT URIs as separate arguments. Each distinct post gets its own list and retry schedule. All posts share the owner’s session and write interval. Cordata resolves a handle to a DID before choosing the list. The list has a title derived from its AT URI and a description that contains the post's DID-based AT URI. When Cordata renames a list whose old title was a handle-based post URI, it keeps that URI in the description.

The `maintaining list` log entry shows the list's AT URI. Subscribe to the list in Bluesky to use its moderation actions. Run `nix run .#cordata -- --help` for all options. Flags override the corresponding `CORDATA_` environment variables, which use uppercase names and underscores in place of hyphens.

## Membership updates

Cordata combines AppView likes with Constellation backlinks. For likers found only through Constellation, it checks the like record on the actor’s PDS. If either scan fails, Cordata can add confirmed likers and retains existing members. After a complete scan, it also removes accounts absent from both sources. Set `--constellation` to use a different backlink endpoint. The owner's repository stores the list and its items across restarts.

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
