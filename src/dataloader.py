import pickle
import os

from torchtext.vocab import build_vocab_from_iterator
from nltk.tokenize import word_tokenize

def yield_tokens(data_iter):
    for text in data_iter:
        yield word_tokenize(text, language="english")

class TorchTextCollator:
    def __init__(self, data):
        print(data.keys())
        ds = []
        for key in ["train", "validation"]:
            for name, comment in zip(
                data[key]["Nom / Libellé"].to_numpy(),
                data[key]["Commentaire réclamation"].to_numpy(),
            ):
                ds.append(name + "\n" + comment)

        self.vocab = build_vocab_from_iterator(
            yield_tokens(ds), specials=["<unk>"]
        )
        self.vocab.set_default_index(self.vocab["<unk>"])
        with open(os.path.join("input", "data", "vocab.pkl"), "wb") as handle:
            pickle.dump(self.vocab, handle, protocol=pickle.HIGHEST_PROTOCOL)

        print("vocab size = " + str(len(self.vocab)))
        self.vocab_size = len(self.vocab)

    def __call__(self, sequences):
        texts = [sequence["text"] for sequence in sequences]
        labels = [sequence["label"] for sequence in sequences]
        # print(labels)
        label_list, text_list, offsets = [], [], [0]
        for (_label, _text) in zip(labels, texts):
            label_list.append(
                (
                    self.alert_mapping[_label[1]],
                    self.default_mapping[_label[2]],
                )
            )
            tokens = word_tokenize(_text, language="french")
            tokens = self.synonym(tokens)
            processed_text = torch.tensor(
                self.vocab(tokens), dtype=torch.int64
            )
            text_list.append(processed_text)
            offsets.append(processed_text.size(0))
        label_list = torch.tensor(label_list, dtype=torch.int64)
        offsets = torch.tensor(offsets[:-1]).cumsum(dim=0)
        text_list = torch.cat(text_list)
        return label_list, text_list, offsets

class QualityClassification(Dataset):
    """
    Class that create the pytorch dataset objects
    """

    def __init__(self, data: pd.DataFrame):
        """
        args:
            data: pandas dataframe object for data
            alert_mapping: dict that maps alerts type to int
            default_mapping: dict that maps default type to int
            model: 'bert-base-uncased' or "distilbert-base-uncased"
        """
        super(QualityClassification, self).__init__()
        self.data = []

        for name, comment in zip(
            data["Nom / Libellé"].to_numpy(),
            data["Commentaire réclamation"].to_numpy(),
        ):
            self.data.append(name + "\n" + comment)
            # self.data.append(name)

        self.notes = data["Note"].to_numpy()
        self.alert_types = data["Type d'alerte (Note≤3)"].to_numpy()
        self.default_types = data["Type Défaut"].to_numpy()

    def __len__(self):
        return len(self.data)

    def __getitem__(self, item):
        return {
            "text": self.data[item],
            "label": [
                self.notes[item],
                self.alert_types[item],
                self.default_types[item],
            ],
        }
