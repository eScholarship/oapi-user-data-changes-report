select
	udc.updated_when,
	udc.user_id,
	case when
		udc.username_old != udc.username_new
			or udc.username_old is null
			or udc.email_old != udc.email_new
			or udc.email_old is null
		then 1 else 0
		end as 'email_or_un_changed',
	udc.pgd_old,
	udc.pgd_new,
	ustd.Affiliation
from
	UCOPReports.user_data_changes udc
		left join [user search term defaults] ustd
			on udc.user_id = ustd.[User ID]
where
	udc.updated_when >= DATEADD(month, -1, GETDATE())
	-- Don't need new users
	and not (
		udc.ucpath_id_old is null
		and udc.email_old is null
		and udc.pgd_old is null
		and udc.username_old is null
	)
	-- only want to see pgd changes
	and udc.pgd_old != udc.pgd_new
order by
	udc.updated_when desc,
	udc.pgd_new,
	udc.user_id