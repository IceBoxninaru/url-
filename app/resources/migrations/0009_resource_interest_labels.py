from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("resources", "0008_resource_interest_feedback"),
    ]

    operations = [
        migrations.AddField(
            model_name="resource",
            name="interest_labels",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
