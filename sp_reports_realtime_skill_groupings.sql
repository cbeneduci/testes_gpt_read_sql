DELIMITER $$
DROP PROCEDURE IF EXISTS sp_reports_realtime_skill_groupings $$
CREATE PROCEDURE `sp_reports_realtime_skill_groupings`(in _json json)
BEGIN


DECLARE _skills TEXT             DEFAULT _json ->> "$.skills";
DECLARE _parametro varchar(20)           DEFAULT _json ->> "$.parametro";
DECLARE _company_id INT                  DEFAULT _json ->> "$.company_id";

SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

CALL meetaone_interface.sp_utils_string_split (_skills, ',');

DROP TEMPORARY TABLE IF EXISTS SKILLS_FILTER_THE_DAY;
CREATE TEMPORARY TABLE SKILLS_FILTER_THE_DAY (
	`Skill_ID`	INT NOT NULL
	,INDEX(Skill_ID)
);

INSERT INTO SKILLS_FILTER_THE_DAY
SELECT VALS FROM string_split;



if _parametro = 'parametro' THEN -- >

    SELECT _parametro, _skills, _company_id;

ELSEIF _parametro = 'charts' THEN

	DROP TEMPORARY TABLE IF EXISTS AGENTS;
	CREATE TEMPORARY TABLE AGENTS (
		 LOGINID INT NOT NULL
		,AWORKMODE INT NOT NULL
		,DIRECTION INT NOT NULL
		,WORKSKILL INT NOT NULL);
		
	INSERT INTO AGENTS 
	SELECT DISTINCT
			 AGENT.LOGINID
            ,AGENT.AWORKMODE
            ,AGENT.DIRECTION
            ,AGENT.WORKSKILL
    FROM
        meetaone_dados.consolidado_agent AGENT
    JOIN SKILLS_FILTER_THE_DAY TS ON TS.Skill_ID = AGENT.skill
        WHERE 1=1
            AND `DATE` = CAST(NOW() as DATE) 
			AND `TIME` = '23:59'
            AND COMPANY_ID = _company_id;
            
           
	
	
	DROP TEMPORARY TABLE IF EXISTS SKILLS;
	CREATE TEMPORARY TABLE SKILLS (
		 sla double NULL
		,queue INT NULL
		,oldest_call_in_queue INT NULL
		,abandoned_calls INT NULL
		,average_wait_time DOUBLE NULL
		,abandoned_calls_percentage DOUBLE NULL
		,offered_calls INT NULL
		,answered_calls INT NULL
		,average_talk_time INT NULL
		,answered_calls_percentage DOUBLE NULL
	);
	
	
	INSERT INTO SKILLS
    SELECT 
         ROUND((SUM(ACCEPTABLE) / (SUM(ACDCALLS) + SUM(ABNCALLS))) * 100, 2) AS sla
        ,SUM(CASE WHEN `TIME` = '23:59' THEN INQUEUE_INRING END) 			AS queue
        ,MAX(OLDESTCALL) 				AS oldest_call_in_queue
        ,SUM(ABNCALLS)					AS abandoned_calls
        ,ROUND(SUM(C_TME * C_RECEBIDAS)/SUM(C_RECEBIDAS),2)         AS average_wait_time 
        ,(SUM(ABNCALLS)/SUM(C_RECEBIDAS)*100)       	AS abandoned_calls_percentage 
        ,SUM(C_RECEBIDAS)                           AS offered_calls
		,SUM(ACDCALLS)                              AS answered_calls
        ,SUM(C_TMA*ACDCALLS)/SUM(ACDCALLS)          AS average_talk_time
        ,(SUM(ACDCALLS)/SUM(C_RECEBIDAS)*100)         AS answered_calls_percentage
    FROM meetaone_dados.consolidado_skill as C_SKILLS
    JOIN SKILLS_FILTER_THE_DAY TS ON TS.Skill_ID = C_SKILLS.skill
    WHERE 1=1
        AND `DATE` = CAST(NOW() as DATE) 
        AND COMPANY_ID = _company_id;
        
                
     SELECT 
            0                                                                                                   AS skill
            ,COUNT(DISTINCT(AGENTS.LOGINID))                                                                    AS logged
            ,SUM(CASE WHEN AGENTS.AWORKMODE = 50 then 1 ELSE 0 end)                                             AS aux
            ,SUM(CASE WHEN AGENTS.AWORKMODE = 30 AND FIND_IN_SET(AGENTS.WORKSKILL,_skills) then 1 ELSE 0 end)   AS talking
            ,SUM(CASE WHEN AGENTS.AWORKMODE = 20 then 1 ELSE 0 end)                                             AS available
            ,SUM(CASE WHEN AGENTS.AWORKMODE = 50 AND AGENTS.DIRECTION = 2 then 1 ELSE 0 END)                    AS aux_out
            ,SUM(CASE WHEN AGENTS.AWORKMODE NOT IN (20,30,50) OR AGENTS.AWORKMODE = 30 AND NOT FIND_IN_SET(AGENTS.WORKSKILL,_skills) THEN 1 ELSE 0 END)                                AS `others`
            ,SKILLS.queue                                                                                       AS queue
            ,SKILLS.oldest_call_in_queue																		AS oldest_call_in_queue
            ,SKILLS.sla                                                                                         AS service_level
            ,SKILLS.average_wait_time																			AS average_wait_time
            ,SKILLS.abandoned_calls																				AS abandoned_calls
            ,ROUND(SKILLS.abandoned_calls_percentage,0)															AS abandoned_calls_percentage
            ,SKILLS.offered_calls																				AS offered_calls
            ,SKILLS.answered_calls																				AS answered_calls
            ,SKILLS.average_talk_time																			AS average_talk_time
			,ROUND(SKILLS.answered_calls_percentage,0)															AS answered_calls_percentage
        FROM SKILLS
		LEFT JOIN AGENTS ON 1=1;

ELSEIF _parametro = 'table' THEN

    WITH AGENTSKILLS AS (
		SELECT 
			AGENT.LOGINID, 
            GROUP_CONCAT(DISTINCT AGENT.SKILL) 	AS SKILLS_CODES, 
            GROUP_CONCAT(DISTINCT SKILLS.`NAME`) AS SKILLS_NAMES
            
        FROM meetaone_dados.consolidado_agent AGENT
        JOIN meetaone_dados.avaya_skills SKILLS
			ON AGENT.SKILL = SKILLS.SKILL 
			AND AGENT.COMPANY_ID = SKILLS.COMPANY_ID
			AND AGENT.CMS_ID = SKILLS.CMS_ID
		WHERE 1=1
			AND `DATE` = CAST(NOW() AS DATE)
			AND `TIME` = '23:59' 
			AND AGENT.COMPANY_ID = _company_id 
		GROUP BY AGENT.LOGINID
	)

	SELECT
         AGENT.LOGINID                                                                                  AS login
        ,AGENT.EXTENSION                                                                                AS extension
        ,ANAMES.AGENTNAME                                                                               AS agent_name
        ,meetaone_dados.avaya_state(AGENT.AWORKMODE)                                                    AS agent_state
        ,CASE WHEN AGENT.AWORKMODE = 50 THEN PAUSE.`NAME` ELSE '' END                                   AS aux_name
        ,AGENT.AGTIME                                                                                   AS state_time
        ,CASE WHEN AGENT.DIRECTION = 2 THEN 'Entrada' 
			WHEN AGENT.DIRECTION = 1 THEN 'Saida' ELSE '' END        									AS call_direction
        ,NULLIF(AGENT.WORKSKILL,0)                                                                      AS workskill
        ,SKILLS.`NAME`                                                                      			AS workskill_name
        ,AGENTSKILLS.SKILLS_CODES																		AS skills
        ,AGENTSKILLS.SKILLS_NAMES																		AS skills_name

    FROM meetaone_dados.consolidado_agent AGENT
	JOIN SKILLS_FILTER_THE_DAY TS ON TS.Skill_ID = AGENT.skill
    LEFT JOIN meetaone_dados.avaya_agents ANAMES 
        ON AGENT.LOGINID = ANAMES.LOGINID 
        AND AGENT.COMPANY_ID = ANAMES.COMPANY_ID
        AND AGENT.CMS_ID = ANAMES.CMS_ID
    LEFT JOIN meetaone_dados.avaya_skills SKILLS
        ON AGENT.WORKSKILL = SKILLS.SKILL 
        AND AGENT.COMPANY_ID = SKILLS.COMPANY_ID
        AND AGENT.CMS_ID = SKILLS.CMS_ID
    LEFT JOIN meetaone_dados.avaya_pause_codes PAUSE
        ON AGENT.AUXREASON = PAUSE.PAUSECODE
        AND AGENT.COMPANY_ID = PAUSE.COMPANY_ID
        AND AGENT.CMS_ID = PAUSE.CMS_ID
    JOIN AGENTSKILLS
        ON AGENT.LOGINID = AGENTSKILLS.LOGINID

    WHERE 1=1
		AND `DATE` = CAST(NOW() AS DATE)
        AND `TIME` = '23:59'
        AND AGENT.COMPANY_ID = _company_id
        
    GROUP BY AGENT.LOGINID;

ELSEIF _parametro = 'info' THEN

SELECT 'Procedure responsável pela Tela 4' as Descricao, 'https://everdata.meeta.com.br/reports/customs/specialty/agents' as Link;

END IF;

SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

END $$
DELIMITER ;