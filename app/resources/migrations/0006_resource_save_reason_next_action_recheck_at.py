from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("resources", "0005_resource_capture_images_resource_capture_videos_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="resource",
            name="next_action",
            field=models.CharField(blank=True, max_length=255),
        ),
        migrations.AddField(
            model_name="resource",
            name="recheck_at",
            field=models.DateField(blank=True, db_index=True, null=True),
        ),
        migrations.AddField(
            model_name="resource",
            name="save_reason",
            field=models.CharField(
                blank=True,
                choices=[
                    ("後で読む", "後で読む"),
                    ("消えそう", "消えそう"),
                    ("参考実装", "参考実装"),
                    ("就活用", "就活用"),
                    ("買い物候補", "買い物候補"),
                ],
                db_index=True,
                max_length=40,
            ),
        ),
    ]
