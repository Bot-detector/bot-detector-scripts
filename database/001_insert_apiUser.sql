insert ignore into apiPermissions (id, permission) values 
(1, 'verify_ban'),
(2, 'verify_players'),
(3, 'request_highscores')
;

insert ignore into apiUser (id, username, token, ratelimit, is_active) values 
(1, 'verify_ban', 'verify_ban', -1, 1),
(2, 'verify_players', 'verify_players', -1, 1),
(3, 'request_highscores', 'request_highscores', -1, 1)
;

insert ignore into apiUserPerms (user_id, permission_id) VALUES
(1,1),
(1,2),
(1,3),
(2,2),
(3,3)
;