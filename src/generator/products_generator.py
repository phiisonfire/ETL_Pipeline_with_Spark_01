import argparse
from faker import Faker
import mysql.connector
from mysql.connector import errorcode
import random
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
