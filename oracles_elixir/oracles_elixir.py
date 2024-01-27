"""
Oracle's Elixir

This script is designed to connect to Tim Sevenhuysen's Oracle's Elixir data.
It is built to empower esports enthusiasts, data scientists, or anyone
to leverage pro game data for use in their own scripts and analytics.

Please visit and support www.oracleselixir.com
Tim provides an invaluable service to the League community.
"""
import datetime as dt
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Union
from dotenv import load_dotenv
from os import getenv

# Housekeeping
import awswrangler as wr
import boto3
import numpy as np
import pandas as pd

load_dotenv()

# Initialize Logger
date_format = "%m/%d/%Y %I:%M:%S %p"
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", datefmt=date_format
)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


# Primary Functions
@dataclass
class OraclesElixir:
    session: Optional[boto3.Session]
    bucket: str

    def ingest_data(
        self, years: Optional[Union[list, str, int]] = None
    ) -> pd.DataFrame:
        """
        Pull data from S3 based on the specified years
            and store it in the `oe_data` instance variable.

        Parameters
        ----------
        years : Union[list, str, int]
            A string or list of strings containing years (e.g. ["2019", "2020"]).
            If nothing is specified, returns the current year only by default.
        """
        if years is None:
            years = [dt.date.today().year]
        if isinstance(years, (str, int)):
            years = [years]

        file_paths = [
            f"s3://{self.bucket}/{year}_LoL_esports_match_data_from_OraclesElixir.csv"
            for year in years
        ]
        logger.info(file_paths)
        oe_data = wr.s3.read_csv(
            file_paths, boto3_session=self.session, low_memory=False
        )

        return oe_data

    @staticmethod
    def format_data_types(oe_data: pd.DataFrame) -> pd.DataFrame:
        """
        Handle data type formatting.
        Dates as dates, nulls as nulls, remove leading/trailing whitespace,
            sets game length as minutes instead of seconds.
        """
        # Format Date Column
        oe_data["date"] = pd.to_datetime(oe_data["date"])

        # Format ID columns as strings
        f_cols = ["gameid", "playerid", "teamid"]
        oe_data[f_cols] = oe_data[f_cols].apply(lambda x: x.str.strip())

        # Handle Null Values
        replace_values = {"": np.nan, "nan": np.nan, "null": np.nan}
        oe_data = oe_data.replace(
            {
                "gameid": replace_values,
                "playerid": replace_values,
                "teamid": replace_values,
                "position": replace_values,
            }
        )

        # Convert Game Length to Minutes
        oe_data["gamelength"] = oe_data["gamelength"] / 60

        return oe_data

    @staticmethod
    def remove_null_games(oe_data: pd.DataFrame) -> pd.DataFrame:
        """
        If a gameID is null, we want to make sure it is not included in the dataset.

        :param oe_data:
        :return:
        """
        return oe_data.dropna(subset=["gameid"])

    @staticmethod
    def drop_unknown_entities(oe_data: pd.DataFrame) -> pd.DataFrame:
        return oe_data[
            (~oe_data["playername"].isin(["unknown player"]))
            & (~oe_data["teamname"].isin(["unknown team"]))
        ]

    @staticmethod
    def subset_data(oe_data: pd.DataFrame, split_on: str) -> pd.DataFrame:
        columns = {
            "team": [
                "date",
                "gameid",
                "side",
                "league",
                "patch",
                "teamname",
                "teamid",
                "result",
                "kills",
                "deaths",
                "assists",
                "egpm",
                "gamelength",
                "ckpm",
                "team kpm",
                "firstblood",
                "dragons",
                "barons",
                "towers",
                "goldat15",
                "xpat15",
                "csat15",
                "golddiffat15",
                "xpdiffat15",
                "csdiffat15",
            ],
            "player": [
                "date",
                "gameid",
                "side",
                "position",
                "league",
                "patch",
                "playername",
                "playerid",
                "teamname",
                "teamid",
                "result",
                "kills",
                "deaths",
                "assists",
                "total cs",
                "egpm",
                "earnedgoldshare",
                "damagetochampions",
                "dpm",
                "damageshare",
                "damagetakenperminute",
                "wardsplaced",
                "wpm",
                "wardskilled",
                "wcpm",
                "controlwardsbought",
                "visionscore",
                "vspm",
                "totalgold",
                "monsterkills",
                "minionkills",
                "gamelength",
                "ckpm",
                "cspm",
                "team kpm",
                "goldat15",
                "xpat15",
                "csat15",
                "killsat15",
                "assistsat15",
                "deathsat15",
                "opp_killsat15",
                "opp_assistsat15",
                "opp_deathsat15",
                "golddiffat15",
                "xpdiffat15",
                "csdiffat15",
            ],
        }
        if split_on in columns:
            oe_data = oe_data.rename(columns={"earned gpm": "egpm"})
            if split_on.lower() == "team":
                oe_data = oe_data[oe_data["position"] == "team"]
            else:
                oe_data = oe_data[oe_data["position"] != "team"]
            return oe_data[columns[split_on]]
        else:
            raise ValueError("Must split on either player or team.")

    @staticmethod
    def remove_inconsistent_games(
        oe_data: pd.DataFrame, split_on: Optional[str]
    ) -> pd.DataFrame:
        """
        Removes entries from the input DataFrame with inconsistent game records based on gameID counts.

        Inconsistencies are determined based on the `split_on` parameter. If `split_on` is "team", games
        with counts not equal to 2 are considered inconsistent. For any other value, games with counts
        not equal to 10 are considered inconsistent.

        Args:
            oe_data (pd.DataFrame): Input DataFrame containing game data with a 'gameid' column.
            split_on (str, optional): Criterion for inconsistency. If "team", inconsistent games are
                                      those with not exactly 2 records. Otherwise, games with not exactly
                                      10 records are considered inconsistent.

        Returns:
            pd.DataFrame: A new DataFrame with inconsistent game records removed.
        """
        counts = oe_data["gameid"].value_counts()
        inconsistent_games = counts[
            (counts != 2) if split_on.lower() == "team" else (counts != 10)
        ].index
        return oe_data[~oe_data["gameid"].isin(inconsistent_games)]

    @staticmethod
    def sort_data(oe_data: pd.DataFrame, split_on: Optional[str]) -> pd.DataFrame:
        if split_on == "player":
            return oe_data.sort_values(["league", "date", "gameid", "side", "position"])
        elif split_on == "team":
            return oe_data.sort_values(["league", "date", "gameid", "side"])

    @staticmethod
    def fill_null_team_ids(oe_data: pd.DataFrame, split_on: str) -> pd.DataFrame:
        if split_on == "player":
            oe_data["teamid"] = oe_data["teamid"].fillna(oe_data["teamname"])
        return oe_data

    @staticmethod
    def enrich_opponent_metrics(oe_data: pd.DataFrame, split_on: str) -> pd.DataFrame:
        metrics = {
            "teamid": oe_data["teamid"].fillna(oe_data["teamname"]),
            "opponentteam": get_opponent(oe_data["teamname"].to_list(), split_on),
            "opponentteamid": get_opponent(oe_data["teamid"].to_list(), split_on),
            "opponent_egpm": get_opponent(oe_data["egpm"].to_list(), split_on),
        }
        if split_on == "player":
            metrics.update(
                {
                    "playerid": oe_data["playerid"].fillna(oe_data["playername"]),
                    "opponentname": get_opponent(
                        oe_data["playername"].to_list(), split_on
                    ),
                    "opponentid": get_opponent(oe_data["playerid"].to_list(), split_on),
                }
            )

        oe_data = oe_data.assign(**metrics)

        return oe_data

    def clean_data(
        self,
        oe_data: pd.DataFrame,
        split_on: Optional[str],
        team_replacements: Optional[Dict] = None,
        player_replacements: Optional[Dict] = None,
    ) -> pd.DataFrame:
        """
        Format and clean data from Oracle's Elixir.
        This function makes the data more consistent and user-friendly

        The date column will be formatted appropriately as a datetime object.
        Any games with 'unknown team' or 'unknown player' will be dropped.
        Any games with null game ids will be dropped.
        Opponent metrics will be enriched into the dataframe.
        Subsets the dataset down to relevant columns for the entity you split on.
        NOTE: Not all data from the initial data set are in the "cleaned" output.

        Parameters
        ----------
        oe_data:
            Pandas DataFrame containing Oracle's Elixir data.
        split_on : 'team', 'player' or None
            Subset data for Team data or Player data. None for all data.
        team_replacements: Optional[dict]
            Replacement values to normalize team names in the data
            if a team name changes over time.
            Format: {'oldname1': 'newname1', 'oldname2': 'newname2'}
        player_replacements: Optional[dict]
            Replacement values to normalize player names in the data
            if a player's name changes over time.
            Format: {'oldname1': 'newname1', 'oldname2': 'newname2'}

        Returns
        -------
        A Pandas dataframe of formatted, subset Oracle's Elixir data matching
        the parameters provided above.
        """
        oe_data = self.format_data_types(oe_data)
        oe_data = self.remove_null_games(oe_data)
        oe_data = self.drop_unknown_entities(oe_data)
        oe_data = self.sort_data(oe_data, split_on)
        oe_data = self.fill_null_team_ids(oe_data, split_on)
        oe_data = self.subset_data(oe_data, split_on)
        oe_data = self.remove_inconsistent_games(oe_data, split_on)
        oe_data = self.enrich_opponent_metrics(oe_data, split_on)
        return oe_data


def get_opponent(column: pd.Series, entity: str) -> list:
    """
    Generate value for the opposing team or player.
    Used for utilities such as returning the opposing player/team's name.
    It can also return opposing metrics, ex: opponent's earned gold per minute
    Be sure that the input value is sorted to have consistent order in rows.

    Parameters
    ----------
    column : Pandas Series
        Pandas DataFrame column representing entity data (see entity)
    entity : str
        'player' or 'team', entity to calculate opponent of.

    Returns
    -------
    opponent : list
        The opponent of the entities in the column provided;
        can be inserted as a column back into the dataframe.
    """
    logger.info(f"Calculating opponent values for {entity}")
    opponent = []
    flag = 0

    # The gap represents how many rows separate a value from its opponent
    # Teams are 1 (Team A, Team B)
    # Players are 5 (ADC to opposing ADC is a 5 row gap)
    gap_dict = {"player": 5, "team": 1}
    gap = gap_dict.get(entity)
    if gap is None:
        raise ValueError("Entity must be either player or team.")

    for i, obj in enumerate(column):
        # If "Blue Side" - fetch opposing team/player below
        if flag < gap:
            opponent.append(column[i + gap])
            flag += 1
        # If "Red Side" - fetch opposing team/player above
        elif gap <= flag < (gap * 2):
            opponent.append(column[i - gap])
            flag += 1
        else:
            raise ValueError(f"Index {i} - Out Of Bounds")

        # After both sides are enumerated, reset the flag
        if flag >= (gap * 2):
            flag = 0
    return opponent


if __name__ == '__main__':
    # Define Time
    time_format = "%Y-%m-%dT%H:%M:%SZ"
    start = dt.datetime.now().strftime(time_format)
    end = (dt.datetime.now() + dt.timedelta(days=30)).strftime(time_format)

    # Download Data
    s3_session = boto3.Session(aws_access_key_id=getenv("ACCESS_ID"),
                               aws_secret_access_key=getenv("SECRET_ID"),
                               )

    oracle = OraclesElixir(session=s3_session,
                           bucket='oracles-elixir')

    data = oracle.ingest_data(years=[str(dt.date.today().year - 1)])
    team_data = oracle.clean_data(data, split_on="player")
    #lpl = len(team_data[team_data["league"] == "LPL"])
    print(team_data)
