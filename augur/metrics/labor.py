#SPDX-License-Identifier: MIT
"""
Metrics that provide data about commits & their associated activity
"""

import datetime
import sqlalchemy as s
import pandas as pd
from augur.util import register_metric

@register_metric()
def laborhours(self, repo_group_id, repo_id=None, begin_date=None, end_date=None, period='month'):
    """
    :param repo_id: The repository's id
    :param repo_group_id: The repository's group id
    :param period: To set the periodicity to 'day', 'week', 'month' or 'year', defaults to 'day'
    :param begin_date: Specifies the begin date, defaults to '1970-1-1 00:00:00'
    :param end_date: Specifies the end date, defaults to datetime.now()
    :return: DataFrame of persons/period
    """
    if not begin_date:
        begin_date = '1970-1-1 00:00:01'
    if not end_date:
        end_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    committersSQL = None

    if repo_id:
        committersSQL = s.sql.text(
            """
                SELECT DATE,
                    repo_name,
                    rg_name,
                    round( SUM ( estimated_labor_hours ), 2 ) AS estimated_hours 
                FROM
                    (
                    SELECT
                        date_trunc(:period, max(repo_labor.rl_analysis_date)::TIMESTAMP::DATE) as date, 
                        repo.repo_id,
                        repo_name,
                        rg_name,
                        programming_language,
                        MAX ( repo_labor.rl_analysis_date) as analysis_date,
                        SUM ( total_lines ) AS repo_total_lines,
                        SUM ( code_lines ) AS repo_code_lines,
                        SUM ( comment_lines ) AS repo_comment_lines,
                        SUM ( blank_lines ) AS repo_blank_lines,
                        AVG ( code_complexity ) AS repo_lang_avg_code_complexity,
                        AVG ( code_complexity ) * SUM ( code_lines ) + 20 AS estimated_labor_hours 
                    FROM
                        repo_labor,
                        repo,
                        repo_groups 
                    WHERE
                        repo.repo_id = :repo_id
                        AND repo.repo_group_id = repo_groups.repo_group_id
--                        AND analysis_date::TIMESTAMP::DATE BETWEEN :begin_date and :end_date
                    GROUP BY
                        repo.repo_id,
                        rg_name,
                        programming_language,
                        repo_name 
                    ORDER BY
                        repo_name,
                        repo.repo_id,
                        programming_language 
                    ) C 
                GROUP BY
                    DATE,
                    repo_id,
                    repo_name, 
                    rg_name 
                ORDER BY
                    repo_name
            """ 

        )

    results = pd.read_sql(committersSQL, self.database, params={'repo_id': repo_id, 
        'repo_group_id': repo_group_id,'begin_date': begin_date, 'end_date': end_date, 'period':period})

    return results