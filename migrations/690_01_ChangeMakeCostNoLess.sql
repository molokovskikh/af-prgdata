DROP function IF EXISTS Usersettings.`MakeCostNoLess`;
CREATE DEFINER=`RootDBMS`@`127.0.0.1` FUNCTION Usersettings.`MakeCostNoLess`(selfCost decimal(11, 2), opponentCost decimal(11, 2)) RETURNS decimal(11,2)
    NO SQL
BEGIN

declare distance decimal(11, 2);



if opponentCost is null then

  return selfCost;

end if;

if selfCost <= 50 then
	return selfCost;
end if;

set distance = ((selfCost - opponentCost) * 100) / opponentCost;



if distance > -2 and distance < 0 then

  return selfCost;

end if;



if distance > -23 and distance < -2 then

  return opponentCost - opponentCost * (floor(5 + rand() * 10) / 1000);

end if;



return selfCost;


END;