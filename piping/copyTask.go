package piping

type CopyTask struct {
	sender  Sender
	control chan bool
}

func NewCopyTask(sender Sender) (cp *CopyTask) {
	cp = &CopyTask{
		sender:  sender,
		control: make(chan bool)}
	return cp
}

func (cp *CopyTask) copy() {
	outputs := cp.sender.GetOutputChannels()
	sc := cp.sender.GetSenderChannel()
	first := <-sc
	for _, channel := range outputs {
		channel <- first
	}
}

func (cp *CopyTask) Start() {
	for {
		select {
		case <-cp.control:
			return
		default:
			cp.copy()
		}
	}
}

func (cp *CopyTask) Stop() {
	cp.control <- true
}
