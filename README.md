# Projeto: P2P com Balanceamento de Carga Dinâmico

## Estrutura do Sistema

### 1. Nó Master
- [ ] Gerenciar lista de Workers conectados
- [ ] Receber requisições simuladas/reais
- [ ] Distribuir tarefas para Workers disponíveis
- [ ] Monitorar carga e disponibilidade dos Workers
- [ ] Detectar saturação (sem Workers livres ou fila grande)
- [ ] Iniciar protocolo para pedir Workers emprestados a Masters vizinhos
- [ ] Gerenciar Workers emprestados (conexão, tarefas, retorno)
- [ ] Comunicar-se com outros Masters via protocolo definido (API, sockets, etc.)

### 2. Nó Worker
- [ ] Conectar-se a um Master (original ou emprestado)
- [ ] Receber e executar tarefas (simulação de trabalho)
- [ ] Desconectar e reconectar a outro Master quando solicitado

### 3. Protocolo de Comunicação
- [ ] Definir mensagens para negociação entre Masters
  - Pedido de ajuda (solicitar Workers)
  - Resposta (aceitar ou recusar)
  - Coordenação do redirecionamento dos Workers
  - Confirmação e finalização da operação
- [ ] Comunicação entre Masters e Workers
  - Autenticação e registro
  - Solicitação e liberação de Workers

## Fluxo do Sistema

- [ ] Receber requisições no Master
- [ ] Distribuir para Workers locais
- [ ] Monitorar carga e detectar saturação
- [ ] Solicitar ajuda a Masters vizinhos quando saturado
- [ ] Receber resposta de Masters vizinhos sobre disponibilidade
- [ ] Redirecionar Workers emprestados para o Master solicitante
- [ ] Processar tarefas com Workers emprestados
- [ ] Retornar Workers ao Master original quando a carga baixar

## Próximos Passos

- [ ] Implementar Master com gerenciamento de Workers e fila de tarefas
- [ ] Desenvolver Worker com capacidade de reconexão
- [ ] Definir e implementar protocolo de comunicação entre Masters
- [ ] Implementar lógica para pedir/emprestar Workers dinamicamente
- [ ] Simular carga e testar balanceamento

