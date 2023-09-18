--Задание №2

select * from pg_catalog.pg_roles pr 

create role netocourier with login password 'NetoSQL2022'

revoke  all privileges on database "postgres" from netocourier

revoke  all privileges on database "postgres" from public

grant connect on database "postgres" to netocourier

grant connect on database "postgres" to public

revoke all  privileges on schema public from netocourier

revoke all  privileges on schema public from public

revoke all  privileges on schema pg_catalog from netocourier

revoke all  privileges on schema pg_catalog from public

revoke all  privileges on schema information_schema from netocourier

revoke all  privileges on schema information_schema from public

grant usage on schema public to netocourier

grant usage on schema pg_catalog to netocourier

grant usage on schema information_schema to netocourier

revoke all on all tables in schema public from netocourier 

revoke all on all tables in schema public from public

revoke all on all tables in schema pg_catalog from netocourier 

revoke all on all tables in schema pg_catalog from public

revoke all on all tables in schema information_schema from netocourier 

revoke all on all tables in schema information_schema from public

grant select,update,insert,delete on all tables in schema public to netocourier

grant select on all tables in schema pg_catalog to netocourier

grant select on all tables in schema information_schema to netocourier

--Задание №

create table "user"
	(id uuid default uuid_generate_v4() primary key not null,
	 last_name varchar(30) not null,
	 first_name varchar(20) not null,
	 dissmissed boolean not null)
	
create table account
	(id uuid default uuid_generate_v4() primary key not null,
	 name varchar(50) not null)	

create table contact
	(id uuid default uuid_generate_v4() primary key not null,
	 last_name varchar(30) not null,
	 first_name varchar(20) not null,
	 account_id uuid not null,
	 foreign key (account_id) references account(id))	

create type st as enum ('В очереди', 'Выполняется', 'Выполнено', 'Отменен')
	
create table courier
	(id uuid default uuid_generate_v4() primary key not null,
	 from_place varchar(80) not null,
	 where_place varchar(80)not null,
	 name varchar(40) not null,
	 account_id uuid not null,
	 contact_id uuid not null,
	 description text,
	 user_id uuid not null,
	 status st not null,
	 created_date date default now() not null,
	 foreign key (account_id) references account(id), 
	 foreign key (contact_id) references contact(id),
	 foreign key (user_id) references "user"(id))
	 
--Задание №4

create procedure insert_test_data(value numeric) as $$
begin
	for i in 1..value
	loop
		insert into account (name)
		values (repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random()+0.04)*33)::integer),((random()+0.5)*2)::integer)::varchar(50));
		insert into "user" (last_name, first_name, dissmissed)
		values (repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random()+0.04)*33)::integer),((random()+0.5))::integer)::varchar(30),
				repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random()+0.04)*33)::integer),((random()+0.5))::integer)::varchar(20),
				(round(random())::int)::boolean);
		if i  <= value
			then commit;
		else 
			rollback;
		end if;
	end loop;
	for i in 1..value * 2
	loop
		insert into contact(last_name, first_name, account_id)
		values (repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random()+0.04)*33)::integer),((random()+0.5))::integer)::varchar(30),
				repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random()+0.04)*33)::integer),((random()+0.5))::integer)::varchar(20),
				(select id from account order by random() limit 1));
		if i  <= value * 2
			then commit;
		else 
			rollback;
		end if;
	end loop;
		for i in 1..value * 5
	loop
		insert into courier (from_place, where_place, name, account_id, contact_id, description, user_id, status, created_date)	
		values (repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random()+0.04)*33)::integer),((random()+0.5)*4)::integer)::varchar(80),
				repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random()+0.04)*33)::integer),((random()+0.5)*4)::integer)::varchar(80),
				repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random()+0.04)*33)::integer),((random()+0.5))::integer)::varchar(30),
				(select id from account order by random() limit 1), (select id from contact order by random() limit 1),
				repeat(substring('абвгдеёжзийклмнопрстуфхцчшщьыъэюя',1,((random()+0.04)*33)::integer),((random()+0.5)*10)::integer),
				(select id from "user" order by random() limit 1),
				(select * from (select unnest(enum_range(null::st))) t1 order by random() limit 1),
				(select now() - interval '1 day' * round(random() * 1000) as timestamp));
		if i  <= value * 5
			then commit;
		else 
			rollback;
		end if;
	end loop;
end; 
$$ language plpgsql;
		
call insert_test_data(100) 

drop procedure insert_test_data(value numeric)

drop table account, "user", contact, courier cascade

--Задание №7

create procedure erase_test_data() as $$
begin
	delete from courier;
	delete from contact;
	delete from account;
	delete from "user";
end; 
$$ language plpgsql;

call erase_test_data() 

drop procedure erase_test_data

--Задание №8

create procedure add_courier(from_place1 varchar, where_place1 varchar, name1 varchar, account_id1 uuid, contact_id1 uuid, description1 text, user_id1 uuid) as $$
begin
	insert into courier (from_place, where_place, "name", account_id, contact_id, description, user_id)	
	values (from_place1, where_place1, name1, account_id1, contact_id1, description1, user_id1);
end; 
$$ language plpgsql;

call add_courier('sdf','sdf','sdf','4af5421a-51fc-41b9-ba81-965052bf6baa', '9168d48b-d9d8-4564-adf0-0f8b1d3f56a7', 'sdfsdf', 'cfe84890-9342-445f-9731-ab63fc003a88')

drop procedure add_courier(from_place1 varchar, where_place1 varchar, name1 varchar, account_id1 uuid, contact_id1 uuid, description1 text, user_id1 uuid)

select * from courier;

--Задание №9

create function get_courier (out id uuid, out from_place varchar, out where_place varchar, out "name" varchar, out account_id uuid,
							 out account varchar, out contact_id uuid, out contact varchar, out description text, out user_id uuid,
							 out "user" varchar, out "status" st, out created_date date) RETURNS SETOF record AS $$
begin
	return QUERY select cr.id, cr.from_place, cr.where_place, cr.name, c.account_id, a.name, cr.contact_id, 
						concat(c.last_name, c.first_name)::varchar as contact, cr.description, cr.user_id,
						concat(c.last_name, c.first_name)::varchar as "user", cr.status, cr.created_date
				 from account a 
					join contact c on c.account_id = a.id
					join courier cr on cr.contact_id = c.id 
					join "user" u on u.id = cr.user_id
				 order by cr.status asc, cr.created_date desc;
END;
$$ LANGUAGE plpgsql;

select * from get_courier()

drop function get_courier()

--Задание №10

create procedure change_status(stat st, id1 uuid) as $$
begin
	update courier 
	set status = stat
	where id = id1;
end; 
$$ language plpgsql;

call change_status ('В очереди', '4fd4392c-4df8-4d72-bfc6-3f0f62e41436')

drop procedure change_status

--Задание №11

create function get_users (out "user" varchar) RETURNS setof varchar AS $$
begin
	return QUERY select concat(last_name, first_name)::varchar as "user"
				 from "user"
				 where dissmissed = false
				 order by last_name;
END;
$$ LANGUAGE plpgsql;

select * from get_users() 

drop function get_users()

--Задание №12

create function get_accounts (out account varchar) RETURNS setof varchar AS $$
begin
	return QUERY select "name" as account
				 from account 
				 order by "name";
END;
$$ LANGUAGE plpgsql;

select * from get_accounts() 

drop function get_accounts()

--Задание №13

create function get_contacts(id1 uuid) returns setof varchar as $$
begin
	if id1 is not null then
	return query  (select concat(last_name, first_name)::varchar as contact
				 						 from contact
				 						 where account_id = id1
										 order by last_name);
	else return next 'Выберите контрагента';
	end if;
end;
$$ language plpgsql;

select get_contacts(null); 
select get_contacts('')

drop function get_contacts

select 'Выберите контрагента'
 
--Задание №14

create view courier_statistic as 
select distinct c.account_id, a."name" as account, 
				count(c.id) over (partition by c.account_id) as count_courier, 
				coalesce(t1.count, 0) as count_complete, 
				coalesce(t2.count, 0) as count_canceled, 
				coalesce(month, 0) as percent_relative_prev_month,
				t6.count as count_where_place, 
				coalesce(t4.count, 0) as count_contact, 
				array_agg as cansel_user_array
from courier c
		join account a on a.id = c.account_id 
		left join (select c.account_id, count(id)
				from courier c 	
				where status = 'Выполнено'
				group by c.account_id) t1 on t1.account_id = c.account_id
		left join (select c.account_id, count(id)
				from courier c 	
				where status = 'Отменен'
				group by c.account_id) t2 on t2.account_id = c.account_id 
		left join (	select account_id, count(id)* 100/ (lag(count(id)) over (order by date_trunc('month', c.created_date))) as month
					from courier c 
					where date_trunc('month', c.created_date) = date_trunc('month', now())
					group by 1, date_trunc('month', c.created_date))  t3 on t3.account_id = c.account_id 					
		left join (select account_id, count(contact_id)
					from courier
					where status = 'Выполнено'
					group by 1) t4 on t4.account_id = c.account_id 
		left join (select account_id, array_agg(user_id)
					from courier c
					where status = 'Отменен'
					group by 1) t5 on t5.account_id = c.account_id
		left join (select account_id, count(distinct where_place)
					from courier c
					group by account_id) t6 on t6.account_id = c.account_id
					
select * from courier_statistic

drop view courier_statistic

