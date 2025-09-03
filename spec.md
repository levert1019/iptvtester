Series title cleaning (input → query):
strip prefix (EN -, |DE|…), drop tech tags (4K/1080p/HDR…), country tags (GB), years, and all Sxx Exx tokens.
Example: EN - The Sopranos S04 E05 Pie-O-My (1999-2007) 4K → query: The Sopranos.

TMDB lookups:
One lookup per “series occurrence” = (prefix + provider_base + cleaned_series_key). Try /search/tv, then search/multi. No episode API calls.

Persisted per-URL info:
season, episode, and ep_name parsed from provider title are saved and reused.

Final M3U formatting:
PREFIX - Series Sxx Eyy EpName (S/E before ep name).
Movies: PREFIX - Movie Title.
No duplicate prefixes, no years, no tech tags.

Group rewrite (movies & series only):
|{PREFIX}| - {TMDB Primary Genre}; on miss → |{PREFIX}| - Uncategorized.

Sorting: Group → Series → Season → Episode.

OK M3U contents: all working streams, even if TMDB missed (those go to Uncategorized).

Never mix series across providers/prefixes: different prefix or provider path ⇒ different occurrence.

Excel: show proper local datetimes for “Last OK” and “Last Checked”.