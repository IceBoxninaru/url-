from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("resources", "0004_resource_last_link_check_at_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="resource",
            name="capture_images",
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name="resource",
            name="capture_videos",
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name="resource",
            name="search_only",
            field=models.BooleanField(db_index=True, default=False),
        ),
    ]
