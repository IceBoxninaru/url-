from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("snapshots", "0003_snapshot_video_assets"),
    ]

    operations = [
        migrations.AlterField(
            model_name="snapshot",
            name="fetch_method",
            field=models.CharField(
                choices=[("http", "HTTP取得"), ("playwright", "ブラウザ取得")],
                default="http",
                max_length=32,
            ),
        ),
    ]
