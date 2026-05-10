from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("resources", "0007_alter_resource_save_reason"),
    ]

    operations = [
        migrations.AddField(
            model_name="resource",
            name="interest_feedback",
            field=models.CharField(
                choices=[
                    ("none", "未評価"),
                    ("interested", "興味あり"),
                    ("not_interested", "興味なし"),
                ],
                db_index=True,
                default="none",
                max_length=32,
            ),
        ),
    ]
