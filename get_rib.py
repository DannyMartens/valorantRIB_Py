import pandas as pd
import numpy as np
import requests
from urllib.parse import urlparse, parse_qs, quote_plus
import json
import sys
import urllib.parse


def get_ribgg(endpoint, query=None, extras=None):
    url = f"https://backend-prod.rib.gg/v1/{endpoint}"
    if query is not None:
        parsed_url = urlparse(url)
        query = parse_qs(parsed_url.query)
        # print(query)
    resp = requests.get(url)
    content = json.loads(resp.text)
    # print(content)
    return content


def get_ribgg_data(endpoint=None, query=None):
    if endpoint is not None:
        resp = get_ribgg(endpoint)
    else:
        sys.exit('must provide an endpoint')

    return resp.get('data')


def list_to_id_df(x, prefix):
    df = pd.DataFrame(x).T.reset_index()
    df.columns = [f'{prefix}_name', f'{prefix}_id']
    return df


def get_all_region_names():
    x = [{'Europe': 1, 'North America': 2, 'Asia-Pacific': 3, 'Latin America': 4,
          'MENA': 5, 'Oceana': 6, 'International': 7}]
    prefix = 'region'
    return list_to_id_df(x, prefix)


def get_all_role_names():
    x = [{'initiator': 1, 'duelist': 2, 'controller': 3, 'sentinel': 4}]
    prefix = 'role'
    return list_to_id_df(x, prefix)


def get_all_agent_names():
    x = [dict(chamber=17, kayo=16, fade=19, sova=4, raze=2, viper=6, jett=12, omen=11, breach=1, sage=9,
              skye=13, brimstone=8, astra=15, killjoy=5, neon=18, cypher=3, reyna=10, phoenix=7, yoru=14, harber=20)]
    prefix = 'agent'
    agents = list_to_id_df(x, prefix)

    z = [dict(chamber=4, kayo=1, fade=1, sova=1, raze=2, viper=3, jett=2, omen=3, breach=1, sage=4,
              skye=1, brimstone=3, astra=3, killjoy=4, neon=2, cypher=4, reyna=2, phoenix=2, yoru=2, harber=1)]
    roles = pd.DataFrame(z).T.reset_index()
    roles.columns = ['agent_name', 'roll_id']
    merged_df = pd.merge(agents, roles, on='agent_name')
    return merged_df


def get_agent_analytics(map_id, region_id, event_id, role_id, patch_id):
    q = dict(map_id=map_id, region_id=region_id, event_id=event_id, role_id=role_id, patch_id=patch_id)
    return get_ribgg("analytics/agents", query=q)


def get_all_map_names():
    x = dict(
        ascent=1,
        haven=7,
        icebox=4,
        bind=3,
        breeze=8,
        fracture=9,
        pearl=10,
        split=2)
    prefix = "map"
    return list_to_id_df(x, prefix)


def get_all_armor_names():
    x = dict(
        light=1,
        heavy=2
    )
    prefix = 'armor'
    list_to_id_df(x, prefix)


def get_composition_analytics(map_id, region_id, event_id, role_id, patch_id):
    q = dict(map_id=map_id, region_id=region_id, event_id=event_id, role_id=role_id, patch_id=patch_id)
    get_ribgg("analytics/compositions", query=q)


def get_map_analytics(region_id, event_id, patch_id):
    q = dict(region_id=region_id, event_id=event_id, patch_id=patch_id)
    get_ribgg("analytics/maps", query=q)


def get_all_weapon_names():
    prefix = "weapon"
    x = dict(
        vandal=4,
        phantom=6,
        classic=11,
        spectre=18,
        sheriff=13,
        ghost=12,
        operator=15,
        bulldog=5,
        frenzy=10,
        stinger=19,
        guardian=16,
        marshal=17,
        judge=8,
        shorty=14,
        odin=2,
        bucky=9,
        ares=3
    ),
    weapons = list_to_id_df(x, prefix)
    categories = pd.DataFrame(
        [dict(ares='heavy', odin='heavy', bulldog='rifle', phantom='rifle', gaurdian='rifle', vandal='rifle',
              judge='shotgun', bucky='shotgun', ghost='sidearm', classic='sidearm', sheriff='sidearm',
              frenzy='sidearm', shorty='sidearm', spectre='smg', stinger='smg', marshal='sniper',
              operator='sniper')]).T.reset_index()
    categories.columns = ['weapon_name', 'weapon_category']

    merged_df = pd.merge(weapons, categories, on='weapon_name')
    return merged_df


def get_weapon_analytics(map_id, side, region_id, event_id, role_id, patch_id):
    q = dict(map_id=map_id, side=side, region_id=region_id, event_id=event_id, role_id=role_id, patch_id=patch_id)
    return get_ribgg("analytics/weapons", query=q)


def get_all_team_names():
    return get_ribgg("teams/all")


def get_team(team_id):
    return get_ribgg(f"teams/{team_id}")


def dataframify_player(x):
    df = pd.DataFrame(x)
    nonscalar = [item for item in kept if len(item) > 1]
    if len(nonscalar) > 0:
        for nm in nonscalar:
            df[nm] = [nonscalar[nm]]
    return df


def get_player(player_id):
    # todo: must test here
    url = f"players/{player_id}"
    res = get_ribgg(url)
    return dataframify_player(res)


def get_events(query, sort_by="startDate", ascending=False, has_series=True, n_results=50):
    """
    returned default results on testing, which is noted in the spec doc made by @tonyelhabr
    :param query:
    :param sort_by:
    :param ascending:
    :param has_series:
    :param n_results:
    :return:
    """
    if query is not None:
        safe_string = quote_plus(query)
        query = f"query={safe_string}"
        url = f"events?{query}sort={sort_by}&sortAscending={ascending}&hasSeries={has_series}&take={n_results}"

        resp = get_ribgg_data(url)
        return resp
    else:
        sys.exit()


def get_series(event_id, completed=True, n_results=50):  # [x]
    url = f"series?take={n_results}&eventIds[]={event_id}&completed={completed}"
    return get_ribgg_data(url)


def get_matches(series_id):  # [x]
    url = f"series/{series_id}"
    return get_ribgg(url)


def get_match_details(match_id):
    """
    fuzz tested BAD todo: fix
    :param match_id:
    :return:
    """
    url = f"matches/{match_id}/details"
    get_ribgg(url)


if __name__ == '__main__':
    print(get_player(2716))
