from django import forms

from tags.models import Tag


class TagForm(forms.ModelForm):
    class Meta:
        model = Tag
        fields = ["name", "color", "sort_order"]
        labels = {
            "name": "名前",
            "color": "色",
            "sort_order": "並び順",
        }
        widgets = {
            "name": forms.TextInput(attrs={"placeholder": "例: AI"}),
            "color": forms.TextInput(attrs={"type": "color"}),
            "sort_order": forms.NumberInput(attrs={"min": 0, "step": 1}),
        }
