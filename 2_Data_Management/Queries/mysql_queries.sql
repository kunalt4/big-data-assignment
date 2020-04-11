/*1*/
select name as surname
from Player_details p left join Team_details t on p.t_id=t.t_id
where minutes < 200 and passes > 100 and team_name like '%ia%';

/*2*/
select *
from Player_details
where shots > 20
order by shots desc;

/*3*/
select name as surname, team_name, minutes
from Player_details p left join Team_details t on p.t_id = t.t_id left join Positions pos on p.pos_id = pos.pos_id
where games > 4 and pos_name = 'goalkeeper';

/*4*/
select count(*) as 'superstar'
from Player_details p left join Team_details t on p.t_id = t.t_id
where ranking < 10 and minutes > 350;

/*5*/
select pos_name as "Position", avg(passes) as "Average Passes"
from Player_details p left join Positions pos on p.pos_id = pos.pos_id
where pos_name = 'midfielder' or pos_name = 'forward'
group by pos_name;

/*6*/
select t1.team_name as "Team 1", t2.team_name as "Team 2", t1.goals_for as "GoalsFor", t1.goals_against as "GoalsAgainst"
from Team_details t1, Team_details t2
where t1.goals_for = t2.goals_for and t1.goals_against = t2.goals_against and t1.team_name < t2.team_name;

/*7*/
select team_name as "Team", (goals_for / goals_against) as "Goals Ratio"
from Team_details
where (goals_for / goals_against) = (select max(goals_for / goals_against)
from Team_details);

/*8*/
select team_name as "Team" , avg(passes) as "Average Passes"
from Team_details t inner join Player_details p on t.t_id = p.t_id inner join Positions pos on p.pos_id = pos.pos_id
where pos_name = 'defender'
group by team_name
having avg(passes)>150
order by avg(passes) desc;
