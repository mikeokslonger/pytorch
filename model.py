from torch.autograd import Variable
import torch


### Hyper Parameters
INPUT_SIZE = NUM_FEATURES = 1
HIDDEN_SIZE = 200
NUM_CLASSES = 2
SEQ_LENGTH = 10
NUM_LAYERS = 1


### Model
class LSTM(torch.nn.Module):
    def __init__(self, num_classes, input_size, hidden_size, num_layers):
        super(LSTM, self).__init__()
        self.num_classes = num_classes
        self.num_layers = num_layers
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.seq_length = SEQ_LENGTH
        self.lstm = torch.nn.LSTM(input_size=input_size, hidden_size=hidden_size, num_layers=num_layers, batch_first=True)
        self.fc = torch.nn.Linear(hidden_size, num_classes)
        self.sig = torch.nn.Sigmoid()

    def forward(self, x):
        ### (LAYERS, BATCH_SIZE, HIDDEN_SIZE)
        h0 = Variable(torch.zeros(self.num_layers, x.size(0), self.hidden_size)) # Hidden
        c0 = Variable(torch.zeros(self.num_layers, x.size(0), self.hidden_size)) # Cell state
        _, (h_out, _) = self.lstm(x, (h0, c0))
        h_out = h_out.view(-1, self.hidden_size)
        out = self.fc(h_out)
        activated = self.sig(out)
        return activated

    
def get_model(file='model202.pt'):
    lstm = LSTM(NUM_CLASSES, INPUT_SIZE, HIDDEN_SIZE, NUM_LAYERS)
    lstm.load_state_dict(torch.load('model10.pt', map_location=lambda a, b: a))
    return lstm