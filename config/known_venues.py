"""
Known-venue lookup table for the NoVA Kids pipeline.

Maps a lowercase venue keyword (substring of location_name, title, or source_url)
to a hint dict that overrides or supplements enrichment.

Rules:
  - "tags" are MERGED (union) with derived tags.
  - "rainy_day_friendly" OVERRIDES the derived value only when the key is present.
  - "city" / "county" are set only when the event has none yet.
"""

from __future__ import annotations

from typing import Any

VenueHint = dict[str, Any]

# Key = lowercase substring matched anywhere in location_name (case-insensitive).
# Order matters: first match wins.
KNOWN_VENUES: list[tuple[str, VenueHint]] = [
    # ── Aquariums / Zoos ────────────────────────────────────────────────────
    ("national aquarium",         {"tags": ["indoor", "museum", "animals"], "rainy_day_friendly": True}),
    ("aquarium",                  {"tags": ["indoor", "museum", "animals"], "rainy_day_friendly": True}),
    ("leesburg animal park",      {"tags": ["outdoor", "animals"],          "rainy_day_friendly": False, "county": "Loudoun"}),
    ("zoo",                       {"tags": ["outdoor", "animals"],          "rainy_day_friendly": False}),
    # ── Museums ─────────────────────────────────────────────────────────────
    ("national children's museum",    {"tags": ["indoor", "museum"],            "rainy_day_friendly": True}),
    ("port discovery",                {"tags": ["indoor", "museum"],            "rainy_day_friendly": True}),
    ("childrens museum",              {"tags": ["indoor", "museum"],            "rainy_day_friendly": True}),
    ("children's science center",     {"tags": ["indoor", "museum", "stem"],    "rainy_day_friendly": True, "city": "Fairfax", "county": "Fairfax"}),
    ("science center",                {"tags": ["indoor", "museum", "stem"],    "rainy_day_friendly": True}),
    ("smithsonian",                   {"tags": ["indoor", "museum"],            "rainy_day_friendly": True, "city": "Washington"}),
    ("natural history",               {"tags": ["indoor", "museum", "nature"],  "rainy_day_friendly": True}),
    ("air and space",                 {"tags": ["indoor", "museum", "stem"],    "rainy_day_friendly": True}),
    ("udvar",                         {"tags": ["indoor", "museum", "stem"],    "rainy_day_friendly": True, "county": "Loudoun"}),
    ("airandspace",                   {"tags": ["indoor", "museum", "stem"],    "rainy_day_friendly": True}),
    ("tudor place",                   {"tags": ["indoor", "museum"],            "rainy_day_friendly": True, "city": "Washington"}),
    ("sully historic site",           {"tags": ["outdoor", "museum"],           "rainy_day_friendly": False, "city": "Chantilly", "county": "Fairfax"}),
    ("manassas museum",               {"tags": ["indoor", "museum"],            "rainy_day_friendly": True, "city": "Manassas", "county": "Prince William"}),
    ("burke museum",                  {"tags": ["indoor", "museum"],            "rainy_day_friendly": True, "county": "Fairfax"}),
    # ── Malls / Indoor Destinations ──────────────────────────────────────────
    ("tysons corner center",          {"tags": ["indoor"],                      "rainy_day_friendly": True, "city": "McLean", "county": "Fairfax"}),
    ("tysons corner",                 {"tags": ["indoor"],                      "rainy_day_friendly": True, "city": "McLean", "county": "Fairfax"}),
    ("fair oaks mall",                {"tags": ["indoor"],                      "rainy_day_friendly": True, "county": "Fairfax"}),
    ("one loudoun",                   {"tags": ["indoor"],                      "rainy_day_friendly": True, "city": "Ashburn", "county": "Loudoun"}),
    ("dulles town center",            {"tags": ["indoor"],                      "rainy_day_friendly": True, "city": "Dulles", "county": "Loudoun"}),
    ("capital one hall",              {"tags": ["indoor", "theater"],           "rainy_day_friendly": True, "city": "Tysons", "county": "Fairfax"}),
    ("tackett's mill",                {"tags": ["indoor"],                      "rainy_day_friendly": True, "county": "Prince William"}),
    # ── Fairfax County Library Branches ─────────────────────────────────────
    ("annandale community library",   {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Annandale",   "county": "Fairfax"}),
    ("annandale library",             {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Annandale",   "county": "Fairfax"}),
    ("burke centre library",          {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Burke",       "county": "Fairfax"}),
    ("burke library",                 {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Burke",       "county": "Fairfax"}),
    ("centreville regional library",  {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Centreville", "county": "Fairfax"}),
    ("centreville library",           {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Centreville", "county": "Fairfax"}),
    ("chantilly regional library",    {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Chantilly",   "county": "Fairfax"}),
    ("chantilly library",             {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Chantilly",   "county": "Fairfax"}),
    ("dan branch library",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Reston",      "county": "Fairfax"}),
    ("dolley madison library",        {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "McLean",      "county": "Fairfax"}),
    ("fairfax city regional library", {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Fairfax",     "county": "Fairfax"}),
    ("george mason regional library", {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Annandale",   "county": "Fairfax"}),
    ("great falls library",           {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Great Falls", "county": "Fairfax"}),
    ("herndon fortnightly library",   {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Herndon",     "county": "Fairfax"}),
    ("herndon library",               {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Herndon",     "county": "Fairfax"}),
    ("john marshall library",         {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Falls Church", "county": "Fairfax"}),
    ("kings park library",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Springfield", "county": "Fairfax"}),
    ("kingstowne branch library",     {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Alexandria",  "county": "Fairfax"}),
    ("kingstowne library",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Alexandria",  "county": "Fairfax"}),
    ("lorton library",                {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Lorton",      "county": "Fairfax"}),
    ("mclean hamlet library",         {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "McLean",      "county": "Fairfax"}),
    ("mclean library",                {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "McLean",      "county": "Fairfax"}),
    ("mount vernon library",          {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Alexandria",  "county": "Fairfax"}),
    ("oak marr branch library",       {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Oakton",      "county": "Fairfax"}),
    ("oak marr library",              {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Oakton",      "county": "Fairfax"}),
    ("patrick henry library",         {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Vienna",      "county": "Fairfax"}),
    ("pohick regional library",       {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Lorton",      "county": "Fairfax"}),
    ("pohick library",                {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Lorton",      "county": "Fairfax"}),
    ("providence district library",   {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Falls Church", "county": "Fairfax"}),
    ("reston regional library",       {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Reston",      "county": "Fairfax"}),
    ("richard byrd library",          {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Springfield", "county": "Fairfax"}),
    ("sherwood regional library",     {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Alexandria",  "county": "Fairfax"}),
    ("sherwood library",              {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Alexandria",  "county": "Fairfax"}),
    ("skyline branch library",        {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Falls Church", "county": "Fairfax"}),
    ("thomas jefferson library",      {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Falls Church", "county": "Fairfax"}),
    ("tysons-pimmit regional library",{"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Falls Church", "county": "Fairfax"}),
    ("pimmit regional library",       {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Falls Church", "county": "Fairfax"}),
    ("woodrow wilson library",        {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Falls Church", "county": "Fairfax"}),
    # ── Arlington County Library Branches ───────────────────────────────────
    ("aurora hills branch",           {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("aurora hills library",          {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("cherrydale branch",             {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("cherrydale library",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("columbia pike branch",          {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("columbia pike library",         {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("glencarlyn branch",             {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("glencarlyn library",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("westover branch",               {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("westover library",              {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("central library arlington",     {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("arlington central library",     {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    # ── Loudoun County Library Branches ─────────────────────────────────────
    ("ashburn library",               {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Ashburn",      "county": "Loudoun"}),
    ("blue ridge library",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Purcellville", "county": "Loudoun"}),
    ("brambleton library",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Ashburn",      "county": "Loudoun"}),
    ("cascades library",              {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Sterling",     "county": "Loudoun"}),
    ("cascades branch",               {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Sterling",     "county": "Loudoun"}),
    ("gum spring library",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Aldie",        "county": "Loudoun"}),
    ("lovettsville library",          {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Lovettsville", "county": "Loudoun"}),
    ("middleburg library",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Middleburg",   "county": "Loudoun"}),
    ("rust library",                  {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Leesburg",     "county": "Loudoun"}),
    ("sterling library",              {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Sterling",     "county": "Loudoun"}),
    ("loco lib",                      {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Loudoun"}),
    # ── Prince William County Library Branches ───────────────────────────────
    ("bull run regional library",     {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Manassas",   "county": "Prince William"}),
    ("chinn park library",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Woodbridge", "county": "Prince William"}),
    ("chinn park regional library",   {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Woodbridge", "county": "Prince William"}),
    ("dale city library",             {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Dale City",  "county": "Prince William"}),
    ("dumfries library",              {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Dumfries",   "county": "Prince William"}),
    ("haymarket library",             {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Haymarket",  "county": "Prince William"}),
    ("independent hill library",      {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Manassas",   "county": "Prince William"}),
    ("montclair library",             {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Woodbridge", "county": "Prince William"}),
    ("nokesville library",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Nokesville", "county": "Prince William"}),
    # ── Generic library keywords (catch-all, lower priority) ─────────────────
    ("public library",       {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("county library",       {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("branch library",       {"tags": ["indoor"],  "rainy_day_friendly": True}),
    # ── Bookstores ───────────────────────────────────────────────────────────
    ("barnes & noble",       {"tags": ["indoor", "storytime"], "rainy_day_friendly": True}),
    ("barnes and noble",     {"tags": ["indoor", "storytime"], "rainy_day_friendly": True}),
    ("scrawl books",         {"tags": ["indoor", "storytime"], "rainy_day_friendly": True, "city": "Reston", "county": "Fairfax"}),
    ("politics and prose",   {"tags": ["indoor", "storytime"], "rainy_day_friendly": True, "city": "Washington"}),
    ("little shop of stories",{"tags": ["indoor", "storytime"], "rainy_day_friendly": True}),
    # ── Fairfax County Recreation Centers ────────────────────────────────────
    ("reston community center",      {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Reston",      "county": "Fairfax"}),
    ("mclean community center",      {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "McLean",      "county": "Fairfax"}),
    ("herndon community center",     {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Herndon",     "county": "Fairfax"}),
    ("cub run recreation center",    {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Chantilly",   "county": "Fairfax"}),
    ("cub run rec center",           {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Chantilly",   "county": "Fairfax"}),
    ("audrey moore rec center",      {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Annandale",   "county": "Fairfax"}),
    ("audrey moore recreation",      {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Annandale",   "county": "Fairfax"}),
    ("spring hill rec center",        {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "McLean",      "county": "Fairfax"}),
    ("spring hill recreation center", {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "McLean",      "county": "Fairfax"}),
    ("spring hill recreation",        {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "McLean",      "county": "Fairfax"}),
    ("south run rec center",          {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Springfield", "county": "Fairfax"}),
    ("south run recreation",          {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Springfield", "county": "Fairfax"}),
    ("lee district rec center",      {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Alexandria",  "county": "Fairfax"}),
    ("franconia rec center",         {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Alexandria",  "county": "Fairfax"}),
    ("sully community center",       {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Chantilly",   "county": "Fairfax"}),
    ("sully district",               {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Chantilly",   "county": "Fairfax"}),
    ("dulles south rec",             {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "South Riding", "county": "Loudoun"}),
    ("oak marr rec center",          {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Oakton",      "county": "Fairfax"}),
    ("rcc hunters woods",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Reston",      "county": "Fairfax"}),
    ("rcc ",                         {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Fairfax"}),
    ("sterling community center",    {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Sterling",    "county": "Loudoun"}),
    ("madison community center",     {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Fairfax"}),
    ("madison community",            {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Fairfax"}),
    ("minnie h. peyton",             {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Fairfax"}),
    ("claude moore rec center",      {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Sterling",    "county": "Loudoun"}),
    ("claude moore recreation",      {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Sterling",    "county": "Loudoun"}),
    # ── Generic community/recreation center (catch-all) ──────────────────────
    ("community center",     {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("recreation center",    {"tags": ["indoor"],  "rainy_day_friendly": True}),
    # ── Ice / Skating ─────────────────────────────────────────────────────────
    ("ice house",            {"tags": ["indoor", "sports"], "rainy_day_friendly": True}),
    ("ice rink",             {"tags": ["indoor", "sports"], "rainy_day_friendly": True}),
    ("skating rink",         {"tags": ["indoor", "sports"], "rainy_day_friendly": True}),
    ("ashburn ice house",    {"tags": ["indoor", "sports"], "rainy_day_friendly": True, "city": "Ashburn", "county": "Loudoun"}),
    ("ashburn ice",          {"tags": ["indoor", "sports"], "rainy_day_friendly": True, "city": "Ashburn", "county": "Loudoun"}),
    ("kettler capitals iceplex", {"tags": ["indoor", "sports"], "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    ("bush tabernacle",      {"tags": ["indoor", "sports"], "rainy_day_friendly": True, "city": "Purcellville", "county": "Loudoun"}),
    # ── Indoor Play ───────────────────────────────────────────────────────────
    ("hyper kidz",           {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Ashburn",      "county": "Loudoun"}),
    ("hyperkidz",            {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Ashburn",      "county": "Loudoun"}),
    ("trampoline",           {"tags": ["indoor", "sports"], "rainy_day_friendly": True}),
    ("jump quest",           {"tags": ["indoor", "sports"], "rainy_day_friendly": True, "county": "Fairfax"}),
    ("skyzone",              {"tags": ["indoor", "sports"], "rainy_day_friendly": True}),
    ("urban air",            {"tags": ["indoor", "sports"], "rainy_day_friendly": True}),
    ("kid city",             {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("play zone",            {"tags": ["indoor"],  "rainy_day_friendly": True}),
    # ── Arts / Studios ────────────────────────────────────────────────────────
    ("paintbar",             {"tags": ["indoor", "arts"],  "rainy_day_friendly": True}),
    ("paint studio",         {"tags": ["indoor", "arts"],  "rainy_day_friendly": True}),
    ("muse ",                {"tags": ["indoor", "arts"],  "rainy_day_friendly": True}),
    ("pottery",              {"tags": ["indoor", "arts"],  "rainy_day_friendly": True}),
    ("artspace herndon",     {"tags": ["indoor", "arts"],  "rainy_day_friendly": True, "city": "Herndon", "county": "Fairfax"}),
    ("workhouse arts",       {"tags": ["indoor", "arts"],  "rainy_day_friendly": True, "city": "Lorton",  "county": "Fairfax"}),
    ("signature theatre",    {"tags": ["indoor", "theater"], "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    # ── Performing Arts / Theaters ────────────────────────────────────────────
    ("alden theatre",        {"tags": ["indoor", "theater"], "rainy_day_friendly": True, "city": "McLean",    "county": "Fairfax"}),
    ("alden theater",        {"tags": ["indoor", "theater"], "rainy_day_friendly": True, "city": "McLean",    "county": "Fairfax"}),
    ("birchmere",            {"tags": ["indoor", "music"],   "rainy_day_friendly": True, "city": "Alexandria", "county": "Alexandria"}),
    ("wolf trap",            {"tags": ["outdoor", "music"],  "rainy_day_friendly": False, "city": "Vienna", "county": "Fairfax"}),
    ("wolftrap",             {"tags": ["outdoor", "music"],  "rainy_day_friendly": False, "city": "Vienna", "county": "Fairfax"}),
    ("theater",              {"tags": ["indoor", "theater"], "rainy_day_friendly": True}),
    ("theatre",              {"tags": ["indoor", "theater"], "rainy_day_friendly": True}),
    # ── Restaurants / Dining ──────────────────────────────────────────────────
    ("restaurant",           {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("rigatoni grill",       {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("mount vernon inn",     {"tags": ["indoor"],  "rainy_day_friendly": True, "city": "Alexandria", "county": "Fairfax"}),
    # ── Nature Centers ────────────────────────────────────────────────────────
    ("nature center",        {"tags": ["indoor", "nature", "museum"], "rainy_day_friendly": True}),
    ("hidden oaks",          {"tags": ["indoor", "nature", "museum"], "rainy_day_friendly": True, "county": "Fairfax"}),
    ("potomac overlook",     {"tags": ["outdoor", "nature"],          "rainy_day_friendly": False, "county": "Arlington"}),
    ("ellanor c. lawrence",  {"tags": ["outdoor", "nature"],          "rainy_day_friendly": False, "city": "Chantilly", "county": "Fairfax"}),
    ("meadowlark botanical", {"tags": ["outdoor", "nature"],          "rainy_day_friendly": False, "city": "Vienna", "county": "Fairfax"}),
    ("meadowlark garden",    {"tags": ["outdoor", "nature"],          "rainy_day_friendly": False, "city": "Vienna", "county": "Fairfax"}),
    # ── NOVA Parks (NVRPA) ────────────────────────────────────────────────────
    ("algonkian regional park",   {"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "city": "Sterling",     "county": "Loudoun"}),
    ("algonkian park",            {"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "city": "Sterling",     "county": "Loudoun"}),
    ("bull run regional park",    {"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "city": "Centreville",  "county": "Fairfax"}),
    ("bull run park",             {"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "city": "Centreville",  "county": "Fairfax"}),
    ("fountainhead regional park",{"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "county": "Fairfax"}),
    ("hemlock overlook",          {"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "city": "Clifton",      "county": "Fairfax"}),
    ("lake fairfax park",         {"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "city": "Reston",       "county": "Fairfax"}),
    ("lake fairfax",              {"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "city": "Reston",       "county": "Fairfax"}),
    ("occoquan regional park",    {"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "city": "Lorton",       "county": "Fairfax"}),
    ("upton hill regional park",  {"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "city": "Arlington",    "county": "Arlington"}),
    ("red rock wilderness",       {"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "city": "Leesburg",     "county": "Loudoun"}),
    # ── Fairfax County Parks ──────────────────────────────────────────────────
    ("frying pan farm",      {"tags": ["outdoor", "nature", "animals"], "rainy_day_friendly": False, "city": "Herndon",    "county": "Fairfax"}),
    ("lake accotink park",   {"tags": ["outdoor", "nature"],            "rainy_day_friendly": False, "city": "Springfield","county": "Fairfax"}),
    ("lake accotink",        {"tags": ["outdoor", "nature"],            "rainy_day_friendly": False, "city": "Springfield","county": "Fairfax"}),
    ("claude moore colonial farm", {"tags": ["outdoor", "nature", "animals"], "rainy_day_friendly": False, "city": "McLean", "county": "Fairfax"}),
    ("sky meadows",          {"tags": ["outdoor", "nature"], "rainy_day_friendly": False, "city": "Delaplane"}),
    # ── Loudoun County / Outer NoVA Farms & Parks ────────────────────────────
    ("great country farms",  {"tags": ["outdoor", "nature", "animals"], "rainy_day_friendly": False, "county": "Loudoun"}),
    ("leesburg animal park", {"tags": ["outdoor", "animals"],           "rainy_day_friendly": False, "city": "Leesburg", "county": "Loudoun"}),
    ("kincaid farmstead",    {"tags": ["outdoor", "nature"],            "rainy_day_friendly": False, "city": "Sterling", "county": "Loudoun"}),
    ("cox farms",            {"tags": ["outdoor", "nature", "animals"], "rainy_day_friendly": False, "city": "Centreville", "county": "Fairfax"}),
    ("franklin park",        {"tags": ["outdoor", "nature"],            "rainy_day_friendly": False, "county": "Loudoun"}),
    ("farm",                 {"tags": ["outdoor", "nature"],            "rainy_day_friendly": False}),
    # ── Schools / Educational ─────────────────────────────────────────────────
    ("montessori",           {"tags": ["indoor"],  "rainy_day_friendly": True}),
    ("lcps",                 {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Loudoun"}),
    ("family academy",       {"tags": ["indoor"],  "rainy_day_friendly": True, "county": "Loudoun"}),
    # ── Sports Facilities ─────────────────────────────────────────────────────
    ("dulles sportsplex",    {"tags": ["indoor"], "rainy_day_friendly": True, "county": "Loudoun"}),
    ("sportsplex",           {"tags": ["indoor"], "rainy_day_friendly": True, "county": "Loudoun"}),
    # ── Toy Libraries / Indoor Play ───────────────────────────────────────────
    ("toy nest",             {"tags": ["indoor"], "rainy_day_friendly": True, "city": "Falls Church", "county": "Fairfax"}),
    # ── Cooking / Culinary Studios ────────────────────────────────────────────
    ("cookology",            {"tags": ["indoor", "cooking"], "rainy_day_friendly": True}),
    ("conche",               {"tags": ["indoor", "cooking"], "rainy_day_friendly": True}),
    # ── Preschools / Educational Centers ──────────────────────────────────────
    ("hope preschool",       {"tags": ["indoor", "preschool"], "rainy_day_friendly": True, "city": "Ashburn", "county": "Loudoun"}),
    # ── Animal Shelters / Welfare ──────────────────────────────────────────────
    ("animal welfare league",{"tags": ["indoor", "animals"], "rainy_day_friendly": True, "city": "Arlington", "county": "Arlington"}),
    # ── Performing Arts / Therapy Centers ──────────────────────────────────────
    ("a place to be",        {"tags": ["indoor", "theater"], "rainy_day_friendly": True}),
    # ── Tea / Social Experiences ───────────────────────────────────────────────
    ("tea with mrs",         {"tags": ["indoor"], "rainy_day_friendly": True}),
    # ── Virtual ───────────────────────────────────────────────────────────────
    ("virtual",              {"tags": ["virtual"],          "rainy_day_friendly": True}),
]


def lookup_venue(location_text: str | None) -> VenueHint | None:
    """
    Return the first matching VenueHint for the given location text, or None.

    Matching is case-insensitive substring against the keys in KNOWN_VENUES.
    Also checks against source_url / title if passed as additional context.
    """
    if not location_text:
        return None
    lower = location_text.lower()
    for keyword, hint in KNOWN_VENUES:
        if keyword in lower:
            return hint
    return None


def lookup_venue_multi(*texts: str | None) -> VenueHint | None:
    """Check multiple text fields (e.g., location_name, title, source_url) in order."""
    for text in texts:
        hint = lookup_venue(text)
        if hint is not None:
            return hint
    return None
