from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("jobs", "0002_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="capturejob",
            name="job_type",
            field=models.CharField(
                choices=[("capture", "取得"), ("ai_enrich", "AI補完")],
                default="capture",
                max_length=32,
            ),
        ),
        migrations.AlterField(
            model_name="capturejob",
            name="status",
            field=models.CharField(
                choices=[
                    ("queued", "待機中"),
                    ("running", "実行中"),
                    ("retry_wait", "再試行待ち"),
                    ("succeeded", "成功"),
                    ("failed", "失敗"),
                ],
                db_index=True,
                default="queued",
                max_length=32,
            ),
        ),
    ]
