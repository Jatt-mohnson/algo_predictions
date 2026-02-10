import requests
import pandas as pd

API_URL = "https://api.underdogfantasy.com/beta/v6/over_under_lines"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def fetch_underdog_data() -> dict:
    response = requests.get(API_URL, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def parse_nba_props(data: dict) -> pd.DataFrame:
    players = pd.DataFrame(data["players"]).rename(columns={"id": "player_id"})
    appearances = pd.DataFrame(data["appearances"]).rename(columns={"id": "appearance_id"})
    over_under_lines = pd.DataFrame(data["over_under_lines"])

    # Filter to NBA players only
    players = players[players["sport_id"] == "NBA"]

    # Merge players with their game appearances
    player_appearances = players.merge(
        appearances, on=["player_id", "position_id", "team_id"], how="inner"
    )

    # Expand the nested options list into rows
    lines = over_under_lines.explode("options").reset_index(drop=True)
    options_df = pd.json_normalize(lines["options"])

    # Rename overlapping columns from options before concat
    options_df = options_df.rename(columns={
        "id": "option_id", "status": "option_status", "updated_at": "option_updated_at",
    })
    lines = pd.concat(
        [lines.drop("options", axis=1).reset_index(drop=True), options_df.reset_index(drop=True)],
        axis=1,
    )

    # Extract stat info from the nested over_under column
    lines["appearance_id"] = lines["over_under"].apply(
        lambda x: x["appearance_stat"]["appearance_id"]
    )
    lines["stat_name"] = lines["over_under"].apply(
        lambda x: x["appearance_stat"]["display_stat"]
    )
    lines["choice"] = lines["choice"].map(
        {"lower": "under", "higher": "over"}
    ).fillna(lines["choice"])

    # Drop suspended lines before merging
    lines = lines[lines["status"] != "suspended"]

    # Select only the columns we need from lines for the merge
    line_cols = ["appearance_id", "stat_name", "stat_value", "choice", "payout_multiplier"]
    existing_line_cols = [c for c in line_cols if c in lines.columns]
    lines = lines[existing_line_cols]

    # Join everything together
    props = player_appearances.merge(lines, on="appearance_id", how="inner")
    props["full_name"] = props["first_name"] + " " + props["last_name"]

    # Keep useful columns
    keep_cols = [
        "full_name", "position_name", "team_id", "stat_name",
        "stat_value", "choice", "payout_multiplier",
    ]
    existing = [c for c in keep_cols if c in props.columns]
    props = props[existing].reset_index(drop=True)

    return props


def main():
    print("Fetching Underdog Fantasy NBA player props...")
    data = fetch_underdog_data()
    df = parse_nba_props(data)
    df.to_csv("underdog_nba_props.csv", index=False)
    print(f"Found {len(df)} NBA player prop lines")
    print(f"Saved to underdog_nba_props.csv")


if __name__ == "__main__":
    main()
