

## 1. install ttyd(using snap is easiest)
https://github.com/tsl0922/ttyd

this will open Unix domain socket file for each session on `/tmp/` directory something like this `/tmp/socket.sock` and must have `666` permission set on current user group 

## 2. install caddy
https://caddyserver.com/docs/install

## 3. setup caddy configuration 
have caddy binary file which is provided through git large file storage service 

according to following template 
setup in following path `/etc/caddy/Caddyfile` 

change port under caddy configuration according to availablity on system and make sure nginx `proxy_pass` is also on same port 

also we can set custom JWT token, under below configuration file, provided we have same token set under `~/path_to_terminal_service/.env` file of rust lang 

```caddy
:8082 {
        # Set this path to your site's directory.
        root * /usr/share/caddy

        # Enable the static file server.
        file_server

        # Another common task is to set up a reverse proxy:
        # reverse_proxy localhost:8080

        # Or serve a PHP site through php-fpm:
        # php_fastcgi localhost:9000
        @sessionPath {
                path_regexp session_id ^/([^/]+)
        }

        route @sessionPath {

                jwtauth {
                        sign_key TkZMNSowQmMjOVU2RUB0bm1DJkU3U1VONkd3SGZMbVk=
                        sign_alg HS256
                        from_query access_token token
                        from_header X-Api-Token
                        from_cookies user_session
                        user_claims aud uid user_id username login
                        #user_claims aud
                }

                rewrite * /
                reverse_proxy unix//tmp/sessions/{re.session_id.1}.sock
        }

        respond "Invalid route" 404
}
```

after setup is complete, setting it as a systemd service with name `caddy-tmux` service is required in order we can run this as a command as follows `caddy run --config /etc/caddy/Caddyfile` 


## 4. setup nginx routing configuration to caddy 
add location routing for nginx according to following configuration within `mizzleterminal.mizzle.io` 

NOTE: make sure nginx points to right interface and port for caddy service 

```nginx
    location /session/ {
        # Rewrite the URL correctly
        rewrite ^/session/(.*)$ /$1 break;

        proxy_pass http://127.0.0.1:8082;  # Use localhost instead of 0.0.0.0

        # WebSocket Support
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # Additional Headers
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
```

## 5. setting params for rust service
essentialy all configuration can be set for rust under `.env` file where important ones are following


`UNIX_SOCKET_FOLDER=/tmp/sessions/` path where you wihs to create socket files, make sure this is same set for caddy to point on that path

`TTYD_SESSION_TIMEOUT=1800` auto stop all the process which are spawned using ttyd running no longer than 30 mins, note this param is in secs 