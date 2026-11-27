# Cordata

Cordata maintains a Bluesky moderation list of accounts that liked one post. The list includes the owner and any accounts the owner follows that liked the post.

## Run

Set `CORDATA_IDENTIFIER` and `CORDATA_APP_PASSWORD` for the list owner. Run Cordata from the repository root.

```sh
nix run .#cordata -- 'https://bsky.app/profile/bsky.app/post/3mzyk34ho4c27'
```

Pass either a Bluesky post link or an AT URI. Cordata resolves a handle to a DID before choosing the list. The list has a title derived from its AT URI and a description that contains the post's DID-based AT URI. When Cordata renames a list whose old title was a handle-based post URI, it keeps that URI in the description.

The `maintaining list` log entry shows the list's AT URI. Subscribe to the list in Bluesky to use its moderation actions. Run `nix run .#cordata -- --help` for all options. Flags override the corresponding `CORDATA_` environment variables, which use uppercase names and underscores in place of hyphens.

## Membership updates

Cordata reads all pages of AppView likes and the owner's list items before changing membership. It adds missing likers, removes accounts absent from the likes inventory, and deletes duplicate list items. The owner's repository stores the list and its items across restarts.

Cordata waits five minutes after a successful pass and one minute after a failed pass. Set `--rescan-interval` to change the delay after successful passes. Set `--write-interval` to change the minimum interval between writes, which defaults to ten seconds.

## Deploy to Kubernetes

Create a Secret named `cordata` with an `app-password` key in the release namespace. Install the chart from the repository root.

```sh
helm upgrade --install cordata ./cordata/charts/cordata \
	--set identifier=you.example.com --set existingSecret=cordata \
	--set-string post='at://bsky.app/app.bsky.feed.post/3mzyk34ho4c27'
```

The [chart values](charts/cordata/values.yaml) cover endpoints and scan and write intervals. The chart runs one replica using the image from `ghcr.io/alyraffauf/terrarium/cordata`. Set `image.tag` to a published `sha-<commit>` tag to pin a release.
