import logging
from os import getenv
import asyncio

import discord
import pandas as pd
import psycopg2
from PIL import Image
from io import BytesIO
from discord.ext import commands
from discord.ui import Select, View, Button
from dotenv import load_dotenv

# Variable Definitions
load_dotenv()
BOT_TOKEN = getenv("DISCORD_TOKEN")

pd.options.display.float_format = "{:,.4f}".format
pd.set_option("display.max_rows", None, "display.max_columns", None)

# Retrieve database configuration from environment variables
DB_HOST = getenv("DB_HOST")
DB_PORT = getenv("DB_PORT")
DB_NAME = getenv("DB_NAME")
DB_USER = getenv("DB_USER")
DB_PASS = getenv("DB_PASS")

# Initialize Logger
date_format = "%m/%d/%Y %I:%M:%S %p"
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", datefmt=date_format
)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


# Instantiate Discord Bot
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.typing = False
intents.presences = False

bot = commands.Bot(command_prefix="!", intents=intents)


# Define league data structure
leagues = []

# Load in today's schedule
df = pd.read_csv(r"C:\Users\matth\PycharmProjects\LoLFantasyDraft\local_data\today_schedule.csv")


class LeagueConfigSelect(Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Scoring Rules", value="rules"),
            discord.SelectOption(label="Draft Duration", value="duration"),
            discord.SelectOption(label="Administrators", value="admins"),
            discord.SelectOption(label="Invite Users", value="invite"),
        ]
        super().__init__(placeholder="Select league configuration", options=options)


# Event listener for when the bot has switched from offline to online
@bot.event
async def on_ready():
    logger.info(f"Logged in as {bot.user.name}")


# Basic command to respond to a message
@bot.command(name="hello")
async def hello(ctx):
    await ctx.send(f"Hello, {ctx.author.name}!")


def is_registered(user_id):
    # Initialize cursor/conn as None
    cursor = None
    connection = None

    try:
        # Connect to the database
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS
        )

        # Create a cursor
        cursor = connection.cursor()

        # Check if the user_id exists in the user_registration table
        cursor.execute("SELECT user_id FROM user_registration WHERE discord_id = %s", (str(user_id),))
        result = cursor.fetchone()

        return result is not None

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error checking registration:", error)

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


@bot.command()
async def register(ctx):
    user_id = ctx.author.id
    cursor = None
    connection = None

    if is_registered(user_id):
        await ctx.send("You are already registered.")
    else:
        await ctx.send("**[OPTIONAL]** If you'd like to, please provide your email address:\n"
                       "*(We will never sell/share your email with third parties, and will only rarely reach out.)*")

        try:
            def check(msg):
                return msg.author == ctx.author

            email_message = await bot.wait_for("message", check=check, timeout=60)  # Wait for user's email input

            # Retrieve the user's email from the message content
            user_email = email_message.content

            # Connect to the database and insert registration data
            connection = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASS
            )

            cursor = connection.cursor()

            cursor.execute(
                "INSERT INTO user_registration (discord_id, username, email, channel_id) VALUES (%s, %s, %s, %s)",
                (str(user_id), str(ctx.author.name), str(user_email), str(ctx.channel.id))
            )

            connection.commit()

            await ctx.send("Registration successful!")

        except asyncio.TimeoutError:
            await ctx.send("Registration timed out. Please try again.")
        except (Exception, psycopg2.DatabaseError) as error:
            await ctx.send("Error registering user: " + str(error))
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


@bot.command()
async def schedule(ctx):
    formatted_schedule = df.drop(["Unnamed: 0"], axis=1).reset_index(drop=True).to_markdown()

    # Send the formatted schedule as a message
    await ctx.send("Upcoming Game Schedule:\n```\n" + formatted_schedule + "```\n")


@bot.command()
async def show_teams(ctx):
    # Create a view to hold the buttons
    view = View()

    # Load team icons as files
    team_blue_icon = discord.File("local_data/team_icons/tl.png", filename="tl.png")
    team_red_icon = discord.File("local_data/team_icons/100t.png", filename="100t.png")

    # Create buttons for each team
    team_blue_button = Button(style=discord.ButtonStyle.primary, label="Team Liquid", custom_id="tl")
    team_red_button = Button(style=discord.ButtonStyle.primary, label="100 Thieves", custom_id="100t")

    # Add buttons to the view
    view.add_item(team_blue_button)
    view.add_item(team_red_button)

    # Send the message with the embedded images and buttons
    await ctx.send(
        content="Who will win?",
        files=[team_blue_icon, team_red_icon],
        view=view
    )


@bot.command()
async def play(ctx):
    user_id = ctx.author.id

    if is_registered(user_id):
        await ctx.send("Let's play the game!")
    else:
        await ctx.send("Please register using !register first.")

# Run the bot with the token
bot.run(BOT_TOKEN)
