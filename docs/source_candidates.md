# Source Candidates — Expanding Beyond Libraries and Rec Centers

Research notes for widening the feed past recurring library and parks
programming. Written alongside the addition of the curated marquee-events
source; this is the backlog of everything that source is *not* meant to cover.

> **Verification status.** The findings below come from web research, not from
> fetching these sites. The session this was written in had no outbound network
> access to any event host, so **no selector, feed URL, or rendering mode here
> has been confirmed against a live page.** Treat every "likely platform" note
> as a hypothesis to check first. Per `README.md § Adding a New Source`, add
> any new source to `sources.yaml` as `enabled: false`, verify with
> `--source <id> --dry-run`, and enable only after the output looks right.

---

## The gap this addresses

As of the 2026-08-03 publish, 389 events came from six sources:

| Source | Events | Share |
|---|---:|---:|
| Loudoun County Public Library | 242 | 62% |
| Fairfax County Park Authority | 91 | 23% |
| Fairfax County Public Library | 20 | 5% |
| Alexandria Library | 18 | 5% |
| Arlington Public Library | 10 | 3% |
| Arlington County Parks & Recreation | 8 | 2% |

211 of those 389 events were tagged `storytime`. The feed was excellent at
"what's on at the library Tuesday morning" and silent on "what should we do
this Saturday" — no fairs, no circus, no farm festivals, no touring shows.

The curated source closes that gap for a handful of marquee events. The
candidates below close it structurally.

---

## Tier 1 — Geographic gaps in categories already approved

These are the clearest wins: counties already in the service area whose parks
and rec departments are simply absent, in a category the source policy already
approves.

| Candidate | Why it matters | Notes to verify |
|---|---|---|
| **Loudoun County Parks, Rec & Community Services** | Loudoun is 62% of the feed but *entirely* library — no parks events at all | Check `loudoun.gov` for a calendar feed; county sites of this vintage are often CivicPlus, which exposes iCal/RSS per calendar |
| **Prince William County Parks & Recreation** | The county is in the county-inference map and the geo bounding box, but has zero sources | `pwcva.gov`; also runs the PWC Fair |
| **City of Alexandria Rec, Parks & Cultural Activities** | Alexandria appears only via its library | `alexandriava.gov`; separate from Alexandria Library |
| **City of Fairfax / Town of Vienna / Town of Herndon / Town of Leesburg** | Town-run festivals, concerts, parades that no county calendar carries | Small sites, often a single events page — cheap scrapers if SSR |

## Tier 2 — Museums & nature centers

`docs/source_rules.md` has approved this category since the beginning, and
**not one scraper exists for it.** Highest value per unit of work.

| Candidate | Notes |
|---|---|
| **Steven F. Udvar-Hazy Center** (Smithsonian, Chantilly) | `airandspace.si.edu`; `known_venues.py` already has hints for it. Smithsonian runs a shared events platform — check for a JSON endpoint |
| **National Air & Space, Natural History, American History** | Same platform; DC but inside the bounding box |
| **Smithsonian's National Zoo** | Already in `known_venues.py`. ZooLights in winter |
| **Children's Science Center Lab** (Fair Oaks) | `childsci.org`; already in `known_venues.py` |
| **Mount Vernon** | `mountvernon.org`; Colonial Market & Fair, Fall Harvest Family Days |
| **Workhouse Arts Center** (Lorton) | `workhousearts.org`; family programming and festivals |

## Tier 3 — Performing arts with dedicated family series

| Candidate | Notes |
|---|---|
| **Wolf Trap — Children's Theatre-in-the-Woods** | `wolftrap.org`. A whole summer family series, entirely absent. Strongest single candidate in this tier |
| **Capital One Hall** (Tysons) | Already in `known_venues.py` as a theater venue |
| **Signature Theatre / Synetic / 1st Stage** | NoVA theaters with family matinees |

## Tier 4 — Already listed but blocked

| Source | Status |
|---|---|
| **NOVA Parks** (`nova_parks`) | In `sources.yaml`, `enabled: false`. Events calendar is fully JS-rendered via Drupal Views AJAX. **Worth revisiting**: NOVA Parks runs Bull Run Festival of Lights, Meadowlark's Winter Walk of Lights, and the regional water parks — exactly the destination content the feed lacks. Needs either a Playwright scraper or a discovered JSON endpoint. Until then, its marquee events are curated-file candidates |
| **Eventbrite** (`eventbrite_nova_family`) | In `sources.yaml`, `enabled: false`, needs `EVENTBRITE_API_KEY`. Would surface commercial family events, but quality control is the hard part — see the family relevance filter |

## Tier 5 — Farms and seasonal attractions

Mostly small WordPress/Squarespace sites with no calendar to speak of. These
are the natural home of the **curated** source rather than scrapers, except
where a farm publishes a real calendar.

Great Country Farms (Bluemont), Ticonderoga Farms (Chantilly), Burnside Farms
(Nokesville), Leesburg Animal Park, Temple Hall Farm.

---

## Events researched and their disposition

Verified during this research pass, with what was done about each:

| Event | Dates (2026) | Disposition |
|---|---|---|
| Circus Vazquez — Tysons II | Aug 7–31 | **Added** to curated file |
| Arlington County Fair | Aug 12–16 | **Added** (free admission, per-day hours) |
| State Fair of Virginia — Doswell | Sep 25 – Oct 4 | **Added**, flagged out-of-region |
| Cox Farms Fall Festival — Centreville | Sep 19 – Nov 8 | **Added** as two entries (Nov hours differ) |
| Prince William County Fair — Manassas | Jul 31 – Aug 8 (disputed) | **Not added.** Sources disagreed on the dates and the run was nearly over. Rule 2 says omit rather than guess |
| Fairfax County Summer Carnival & Fair | Jul 30 – Aug 2 | Past. Candidate for next year |
| Loudoun County Fair — Leesburg | Jul 21–25 | Past. Candidate for next year |
| Bull Run Festival of Lights / Winter Walk of Lights | 2026–27 dates unannounced | Revisit in the fall; only 2025–26 dates were findable |
| Mount Vernon Colonial Market & Fair | Sep 13–14 | Candidate — better served by a Mount Vernon scraper (Tier 2) |
| Fall Festival, Historic Old Town Fairfax | Oct 10 | Candidate — better served by a City of Fairfax scraper (Tier 1) |

---

## Recurring annual calendar

Marquee events cluster in predictable windows. Suggested refresh points for
`config/curated_events.yaml`:

| When | Verify and add |
|---|---|
| **June** | County fairs (Loudoun, Fairfax, Prince William, Arlington), summer circus tours |
| **August** | Farm fall festivals, the state fair, fall town festivals |
| **October** | Holiday light trails, `ZooLights`, winter touring shows |
| **February** | Spring break camps, egg hunts, spring festivals |
