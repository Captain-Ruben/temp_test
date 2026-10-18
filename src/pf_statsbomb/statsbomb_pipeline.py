import warnings
import logging
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret
from statsbombpy import sb

# Onderdruk alle warnings en logs van derden
warnings.filterwarnings("ignore")
logging.getLogger("statsbombpy").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)

SELECTED_COLUMNS = [
    # 1. Identificatie & Context
    "id",
    "match_id",
    "index",
    "type",
    "play_pattern",
    "related_events",
    "tactics",
    
    # 2. Tijd & Volgorde
    "period",
    "timestamp",
    "minute",
    "second",
    "duration",
    "injury_stoppage_in_chain",

    # 3. Teams & Spelers
    "possession",
    "possession_team",
    "possession_team_id",
    "team",
    "team_id",
    "player",
    "player_id",
    "position",

    # 4. Generieke Event Attributen
    "location",
    "under_pressure",
    "counterpress",
    "out",
    "off_camera",

    # 5. Passes
    "pass_recipient",
    "pass_recipient_id",
    "pass_type",
    "pass_outcome",
    "pass_height",
    "pass_body_part",
    "pass_length",
    "pass_angle",
    "pass_end_location",
    "pass_technique",
    "pass_cross",
    "pass_cut_back",
    "pass_switch",
    "pass_through_ball",
    "pass_inswinging",
    "pass_outswinging",
    "pass_straight",
    "pass_deflected",
    "pass_aerial_won",
    "pass_assisted_shot_id",
    "pass_shot_assist",
    "pass_goal_assist",
    "pass_miscommunication",
    "pass_no_touch",

    # 6. Schoten (Shots)
    "shot_type",
    "shot_outcome",
    "shot_statsbomb_xg",
    "shot_technique",
    "shot_body_part",
    "shot_end_location",
    "shot_first_time",
    "shot_one_on_one",
    "shot_aerial_won",
    "shot_open_goal",
    "shot_redirect",
    "shot_deflected",
    "shot_saved_to_post",
    "shot_saved_off_target",
    "shot_key_pass_id",
    "shot_freeze_frame",

    # 7. Doelman (Goalkeeper)
    "goalkeeper_type",
    "goalkeeper_position",
    "goalkeeper_body_part",
    "goalkeeper_technique",
    "goalkeeper_outcome",
    "goalkeeper_end_location",
    "goalkeeper_shot_saved_to_post",
    "goalkeeper_shot_saved_off_target",
    "goalkeeper_punched_out",
    "goalkeeper_lost_out",
    "goalkeeper_lost_in_play",
    "goalkeeper_success_out",

    # 8. Overtredingen & Kaarten (Fouls & Bad Behaviour)
    "foul_committed_type",
    "foul_committed_card",
    "foul_committed_advantage",
    "foul_committed_offensive",
    "foul_committed_penalty",
    "foul_won_advantage",
    "foul_won_defensive",
    "foul_won_penalty",
    "bad_behaviour_card",

    # 9. Dribbels, Duels & Balbezit (Dribbles, Duels, Carries, Recoveries)
    "dribble_outcome",
    "dribble_nutmeg",
    "dribble_overrun",
    "dribble_no_touch",
    "duel_type",
    "duel_outcome",
    "carry_end_location",
    "ball_receipt_outcome",
    "ball_recovery_recovery_failure",
    "ball_recovery_offensive",
    "miscontrol_aerial_won",
    "50_50",

    # 10. Verdedigende Acties (Clearances, Blocks, Interceptions)
    "clearance_body_part",
    "clearance_head",
    "clearance_left_foot",
    "clearance_right_foot",
    "clearance_other",
    "clearance_aerial_won",
    "block_deflection",
    "block_offensive",
    "block_save_block",
    "interception_outcome",

    # 11. Wissels & Overige (Substitutions & Misc)
    "substitution_outcome",
    "substitution_outcome_id",
    "substitution_replacement",
    "substitution_replacement_id",
    "player_off_permanent"
]

@task
def fetch_matches(competition_id: int, season_id: int) -> list:
    logger = get_run_logger()
    logger.info("Fetching match list...")
    matches_df = sb.matches(competition_id=competition_id, season_id=season_id)
    return matches_df.to_dict(orient="records")

@task
def process_and_save_events(match: dict, db_url: str, is_first_match: bool):
    logger = get_run_logger()
    match_id = match['match_id']
    logger.info(f"Processing events for match {match_id}...")
    
    df = sb.events(match_id=match_id).sort_values(by=['minute', 'second'])
    df_reindexed = df.reindex(columns=SELECTED_COLUMNS)
        
    for col in df_reindexed.columns:
        if col not in {"match_id", "index", "period", "minute", "second", "possession", "possession_team_id", "team_id", "player_id"}:
            df_reindexed[col] = df_reindexed[col].astype(str).replace({"nan": None, "None": None, "<NA>": None})
            
    df_reindexed.to_sql(name="statsbomb_bronze", con=create_engine(db_url), if_exists="replace" if is_first_match else "append", index=False)
    logger.info(f"Saved events for match {match_id} to PostgreSQL table 'statsbomb_bronze'.")

@flow
def statsbomb_pipeline(competition_id: int = 11, season_id: int = 22):
    for i, match in enumerate(fetch_matches(competition_id, season_id)):
        is_first = (i == 0)
        process_and_save_events(match, db_url=Secret.load("database-public-url").get(), is_first_match=is_first)
        
    logger = get_run_logger()
    logger.info("Pipeline completed successfully!")

if __name__ == "__main__":
    statsbomb_pipeline.from_source(
        source=GitRepository(
            url="https://github.com/Captain-Ruben/temp_test.git",
            branch="master" 
        ),
        entrypoint="statsbomb_pipeline.py:statsbomb_pipeline",  # Vervang met de juiste bestandsnaam
    ).deploy(
        name="statsbomb-bronze-deployment",
        work_pool_name="default-process-pool",
    )