import logging
import pymysql
from pymysql import Error
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def getConnection():

    mysqlConnection = pymysql.connect(host=os.environ['MYSQL_HOST'],
                        user=os.environ['MYSQL_USER'],
                        passwd=os.environ['MYSQL_PWD'],
                        port=int(3306),
                        db=os.environ['MYSQL_DB'],
                        autocommit=True)

    return mysqlConnection

def addVendorMapping(vendor_id, player_id, vendor_player_id, league_id):

    connection = getConnection()

    try:
        cursor = connection.cursor()
        sql = "INSERT INTO `vendor_player_association` VALUES (%s, %s, %s, %s, %s, NOW(), NOW())"
        logger.info(sql)
        cursor.execute(sql, (0, vendor_id , player_id, vendor_player_id, league_id))

    except Error as e:
        logger.warning(e)
        logger.warning("Failed to insert vendor mapping: "+vendor_player_id)
    finally:
        connection.close()

def getCurrentSeason(league):

    connection = getConnection()

    try:
        cursor = connection.cursor()
        query = ("SELECT s.year, st.season_type FROM season s, league l, season_type st "
                 "WHERE s.league_id = l.id and l.abbrev = %s AND s.current_ind = 1 "
                 "AND now() BETWEEN season_start_dt AND season_end_dt AND s.season_type_id = st.id")
        cursor.execute(query,(league))
        rec = cursor.fetchone()
        seasonYear = rec[0]
        seasonType = rec[1]

        return seasonYear, seasonType

    except:
        logger.error("Failed to get current seasonId for league: "+league)
    finally:
        connection.close()
