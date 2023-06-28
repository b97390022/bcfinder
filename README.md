# BCFinder

BCFinderis a Python library scheduling to find Badminton course information,
and push notification to line group.

[![Docker Build](https://github.com/b97390022/bcfinder/actions/workflows/basic.yml/badge.svg)](https://github.com/b97390022/bcfinder/actions/workflows/basic.yml)

## Create Confin.json
Please place the config.json file into the bcfinder folder and replace the corresponding values.

```json
{
    "line_admin_id": "your_admin_line_id",
    "line_group_chat_id": "your_group_chat_id",
    "line_channel_access_token": "your_token",
    "line_channel_secret": "your_secret",
    "reurl_post_uri": "https://api.reurl.cc/shorten",
    "reurl_api_key": "reurl_api_key",
    "default_schedule_job_interval": 86400,
    "tz": "Asia/Taipei"
}
```
## SQLite bcdb.db file
If you have the bcdb.db file, please place it into the bcfinder folder. If you don't have it, remember to remove the mount from the docker-compose.yml file.

## Usage

```bash
git clone https://github.com/b97390022/bcfinder.git
cd bcfinder

docker compose up -d
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)