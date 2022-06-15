select query.server_date_utc,
       query.login,
       CAST(CASE
                WHEN bid_func = 'set_referrer' THEN '-2/* hey?*/'
                WHEN LENGTH(partner_id) > ? THEN partner_id
                ELSE alternate_partner_id END as float) AS partner_id,

WHERE x;