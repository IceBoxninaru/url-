from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("resources", "0006_resource_save_reason_next_action_recheck_at"),
    ]

    operations = [
        migrations.AlterField(
            model_name="resource",
            name="save_reason",
            field=models.CharField(blank=True, db_index=True, max_length=40),
        ),
    ]
