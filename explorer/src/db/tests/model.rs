use crate::db::{
    chain_storable::{Address, BlockDate, BlockId, StorableHash, VotePlanId},
    schema::BlockMeta,
};
use chain_core::property::Fragment as _;
use chain_crypto::{PublicKey, SecretKey};
use chain_impl_mockchain::{
    block::HeaderId,
    certificate::{VoteCast, VotePlan},
    config::ConfigParam,
    fragment::Fragment,
    transaction::{UtxoPointer, Witness},
    value::Value,
    vote::{self, PayloadType},
};

use super::*;
use std::{collections::BTreeMap, convert::TryFrom};

#[derive(Debug, Clone)]
pub enum TaggedKeyPair {
    Utxo(KeyPair<Ed25519>),
    Account(KeyPair<Ed25519>),
}

impl TaggedKeyPair {
    fn to_address(&self) -> chain_addr::Address {
        let kind = match self {
            TaggedKeyPair::Utxo(key_pair) => Kind::Single(key_pair.public_key().clone()),
            TaggedKeyPair::Account(key_pair) => Kind::Account(key_pair.public_key().clone()),
        };
        chain_addr::Address(Discrimination::Test, kind)
    }
}

#[derive(Debug, Clone)]
pub struct State {
    pub block_id: BlockId,
    pub block0_id: BlockId,
    pub keys: Vec<TaggedKeyPair>,
    pub utxo: BTreeMap<PublicKey<Ed25519>, BTreeSet<UtxoPointer>>,
    pub accounts: BTreeMap<PublicKey<Ed25519>, Value>,
    pub fragments: Vec<Fragment>,
    pub transactions_by_address: BTreeMap<Address, Vec<FragmentId>>,
    pub vote_plans: BTreeMap<VotePlanId, VotePlan>,
    pub parent: Option<BlockId>,
}

pub fn initial_state(nkeys: u8) -> State {
    let block0_id = StorableHash::from([1u8; 32]);

    let keys: Vec<TaggedKeyPair> = std::iter::repeat(())
        .enumerate()
        .map(|(i, _)| {
            let key = SecretKey::<Ed25519>::from_binary(&[i as u8; 32])
                .unwrap()
                .into();

            if i % 2 == 0 {
                TaggedKeyPair::Utxo(key)
            } else {
                TaggedKeyPair::Account(key)
            }
        })
        .take(nkeys as usize)
        .collect();

    let addresses = keys.iter().map(|key| key.to_address()).collect::<Vec<_>>();

    let mut config_params = ConfigParams::new();

    config_params.push(ConfigParam::ConsensusVersion(ConsensusType::GenesisPraos));
    config_params.push(ConfigParam::Discrimination(
        chain_addr::Discrimination::Test,
    ));
    config_params.push(ConfigParam::EpochStabilityDepth(2));

    let mut initial_fragments = vec![Fragment::Initial(config_params)];

    let mut utxo: BTreeMap<PublicKey<Ed25519>, BTreeSet<UtxoPointer>> = Default::default();
    let mut accounts: BTreeMap<PublicKey<Ed25519>, Value> = Default::default();
    let mut transactions_by_address: BTreeMap<Address, Vec<FragmentId>> = BTreeMap::new();

    for (i, address) in addresses.iter().enumerate() {
        let output = Output::from_address(address.clone(), Value(10000));

        let tx = TxBuilder::new()
            .set_nopayload()
            .set_ios(&[], &[output])
            .set_witnesses_unchecked(&[])
            .set_payload_auth(&());

        let fragment = Fragment::Transaction(tx);

        transactions_by_address
            .entry(address.into())
            .or_default()
            .insert(0, fragment.id().into());

        match &keys[i] {
            TaggedKeyPair::Utxo(key_pair) => {
                let utxo_pointer = UtxoPointer {
                    transaction_id: fragment.id(),
                    output_index: 0,
                    value: Value(100000),
                };

                utxo.entry(key_pair.public_key().clone())
                    .or_default()
                    .insert(utxo_pointer);
            }
            TaggedKeyPair::Account(key_pair) => {
                accounts.insert(key_pair.public_key().clone(), Value(100000));
            }
        }

        initial_fragments.push(fragment);
    }

    let mut proposals = chain_impl_mockchain::certificate::Proposals::new();

    for proposal_id in 0u8..3 {
        let _ = proposals.push(chain_impl_mockchain::certificate::Proposal::new(
            [proposal_id; 32].into(),
            chain_impl_mockchain::vote::Options::new_length(3u8).unwrap(),
            chain_impl_mockchain::certificate::VoteAction::OffChain,
        ));
    }

    let vote_plan = VotePlan::new(
        chain_impl_mockchain::block::BlockDate {
            epoch: 0,
            slot_id: 0,
        },
        chain_impl_mockchain::block::BlockDate {
            epoch: 100,
            slot_id: 100,
        },
        chain_impl_mockchain::block::BlockDate {
            epoch: 100,
            slot_id: 100,
        },
        proposals,
        PayloadType::Public,
        vec![],
    );

    let vote_plans = {
        let mut vote_plans = BTreeMap::new();
        vote_plans.insert(
            <[u8; 32]>::from(vote_plan.to_id()).into(),
            vote_plan.clone(),
        );

        vote_plans
    };

    let builder = TxBuilder::new()
        .set_payload(&vote_plan)
        .set_ios(&[], &[])
        .set_witnesses_unchecked(&[]);

    let (key, id) = match &keys[0] {
        TaggedKeyPair::Utxo(key_pair) => (
            key_pair.private_key().clone(),
            key_pair.public_key().clone(),
        ),
        TaggedKeyPair::Account(key_pair) => (
            key_pair.private_key().clone(),
            key_pair.public_key().clone(),
        ),
    };

    let auth_data = builder.get_auth_data();
    let signature =
        transaction::SingleAccountBindingSignature::new(&auth_data, |d| key.sign_slice(&d.0));

    let proof = chain_impl_mockchain::certificate::VotePlanProof {
        id: id.into(),
        signature,
    };

    let tx = builder.set_payload_auth(&proof);

    let fragment = Fragment::VotePlan(tx);

    initial_fragments.push(fragment);

    State {
        keys,
        accounts,
        utxo,
        transactions_by_address,
        fragments: initial_fragments,
        vote_plans,
        block_id: block0_id.clone(),
        block0_id,
        parent: None,
    }
}

#[derive(Debug, Clone)]
pub enum TransactionSpec {
    Transaction {
        from: Vec<(usize, u64)>,
        to: Vec<usize>,
    },
    VoteCast {
        from: usize,
        vote_plan: usize,
        proposal: u8,
        option: u8,
    },
}

pub fn new_state(prev: &State, block_id: BlockId, specs: Vec<TransactionSpec>) -> State {
    let mut accounts = prev.accounts.clone();
    let mut utxo = prev.utxo.clone();
    let mut transactions_by_address = prev.transactions_by_address.clone();
    let mut fragments: Vec<Fragment> = vec![];

    for spec in specs {
        match spec {
            TransactionSpec::Transaction { from, to } => {
                make_simple_tx(
                    from,
                    prev,
                    &mut utxo,
                    &mut accounts,
                    to,
                    &mut fragments,
                    &mut transactions_by_address,
                );
            }
            TransactionSpec::VoteCast {
                from,
                vote_plan,
                proposal,
                option,
            } => match &prev.keys[from] {
                TaggedKeyPair::Utxo(_) => {
                    unreachable!("can't cast vote from utxo address")
                }
                TaggedKeyPair::Account(key_pair) => {
                    let len = prev.vote_plans.len();
                    let (vote_plan_id, vote_plan) =
                        prev.vote_plans.iter().nth(vote_plan % len).unwrap();

                    let proposal_index =
                        proposal % u8::try_from(vote_plan.proposals().len()).unwrap();

                    let choice = option
                        % vote_plan.proposals()[proposal_index as usize]
                            .options()
                            .choice_range()
                            .end;

                    let vote_cast = VoteCast::new(
                        vote_plan_id.clone().0.into(),
                        proposal_index,
                        vote::Payload::Public {
                            choice: vote::Choice::new(choice),
                        },
                    );

                    let input = Input::from_account_public_key(
                        key_pair.public_key().clone(),
                        Value::zero(),
                    );

                    let builder = TxBuilder::new()
                        .set_payload(&vote_cast)
                        .set_ios(&[input], &[]);

                    let witness = Witness::new_account(
                        &HeaderId::from_bytes(prev.block0_id.clone().into()),
                        &builder.get_auth_data_for_witness().hash(),
                        // TODO: the explorer doesn't care about the spending counter, so it's find
                        // to just set it always to 0, but we may need to change this, if we end up
                        // using the actual ledger for this.
                        0u32.into(),
                        |data| key_pair.private_key().sign(data),
                    );

                    let tx = builder.set_witnesses(&[witness]).set_payload_auth(&());

                    let fragment = Fragment::VoteCast(tx);

                    transactions_by_address
                        .entry(prev.keys[from].to_address().into())
                        .or_default()
                        .insert(0, fragment.id().into());

                    fragments.push(fragment);
                }
            },
        }
    }

    dbg!(State {
        keys: prev.keys.clone(),
        block0_id: prev.block0_id.clone(),
        accounts,
        utxo,
        transactions_by_address,
        vote_plans: prev.vote_plans.clone(),
        fragments,
        parent: Some(prev.block_id.clone()),
        block_id,
    })
}

fn make_simple_tx(
    from: Vec<(usize, u64)>,
    prev: &State,
    utxo: &mut BTreeMap<PublicKey<Ed25519>, BTreeSet<UtxoPointer>>,
    accounts: &mut BTreeMap<PublicKey<Ed25519>, Value>,
    to: Vec<usize>,
    fragments: &mut Vec<Fragment>,
    transactions_by_address: &mut BTreeMap<Address, Vec<StorableHash>>,
) {
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let mut add_fragment_to = Vec::new();
    for (id, val) in from.iter().cloned() {
        match &prev.keys[id] {
            TaggedKeyPair::Utxo(from) => {
                let utxo_set = utxo.get(from.public_key()).unwrap();

                if let Some(utxo_pointer) = utxo
                    .get(from.public_key())
                    .unwrap()
                    .iter()
                    .nth(val as usize % utxo_set.len())
                    .cloned()
                {
                    utxo.entry(from.public_key().clone()).and_modify(|v| {
                        v.remove(&utxo_pointer);
                    });

                    let transfer = (val % utxo_pointer.value.0) + 1;

                    if let Some(change) = utxo_pointer.value.0.checked_sub(transfer) {
                        let change = Output::from_address(
                            TaggedKeyPair::Utxo(from.clone()).to_address(),
                            Value(change),
                        );

                        outputs.push(change);
                    }

                    let input = Input::from_utxo(utxo_pointer);

                    inputs.push(input);
                    add_fragment_to.push(id);
                }
            }
            TaggedKeyPair::Account(from) => {
                let funds = accounts.get_mut(&from.public_key()).unwrap();
                if funds.0 > 0 {
                    let amount = (val as u64 % funds.0) + 1;

                    *funds = funds.checked_sub(Value(amount)).unwrap();

                    let input =
                        Input::from_account_public_key(from.public_key().clone(), Value(amount));

                    inputs.push(input);
                    add_fragment_to.push(id);
                }
            }
        }
    }
    let total_input = inputs.iter().fold(Value(0), |accum, input| {
        accum.checked_add(input.value()).unwrap()
    });
    let change_output = outputs.iter().fold(Value(0), |accum, output| {
        accum.checked_add(output.value).unwrap()
    });
    let input_to_distribute = total_input.checked_sub(change_output).unwrap();
    let input_per_part = input_to_distribute.0 / (to.len() as u64);
    let mut add_one = to.len() > 1 && (input_to_distribute.0 % 2) == 1;
    for id in to {
        let to = &prev.keys[id];

        let value = Value(if add_one {
            input_per_part + 1
        } else {
            input_per_part
        });
        add_one = false;

        let output = Output::from_address(to.to_address(), value);

        outputs.push(output);
        add_fragment_to.push(id);
    }

    let tx_builder = TxBuilder::new()
        .set_nopayload()
        .set_ios(inputs.as_ref(), outputs.as_ref());

    let sign_data_hash: TransactionSignDataHash = tx_builder.get_auth_data_for_witness().hash();
    let mut witnesses = Vec::new();
    for (id, _) in from.iter().cloned() {
        match &prev.keys[id] {
            TaggedKeyPair::Utxo(key_pair) => witnesses.push(Witness::new_utxo(
                &HeaderId::from_bytes(prev.block0_id.clone().into()),
                &sign_data_hash,
                |data| key_pair.private_key().sign(data),
            )),
            TaggedKeyPair::Account(key_pair) => witnesses.push(Witness::new_account(
                &HeaderId::from_bytes(prev.block0_id.clone().into()),
                &sign_data_hash,
                // TODO: the explorer doesn't care about the spending counter, so it's find
                // to just set it always to 0, but we may need to change this, if we end up
                // using the actual ledger for this.
                0u32.into(),
                |data| key_pair.private_key().sign(data),
            )),
        }
    }
    let tx = tx_builder
        .set_witnesses_unchecked(witnesses.as_ref())
        .set_payload_auth(&());

    assert_eq!(tx.total_input(), tx.total_output());

    let fragment = Fragment::Transaction(tx);
    fragments.push(fragment.clone());

    for output in outputs {
        match output.address.kind() {
            Kind::Single(to) => {
                utxo.entry(to.clone()).and_modify(|v| {
                    v.insert(UtxoPointer {
                        transaction_id: fragment.id(),
                        output_index: 0,
                        value: output.value,
                    });
                });
            }
            Kind::Group(_, _) => {}
            Kind::Account(to) => {
                accounts.entry(to.clone()).and_modify(|v| {
                    *v = v.checked_add(output.value).unwrap();
                });
            }
            Kind::Multisig(_) => {}
            Kind::Script(_) => {}
        }
    }

    for id in add_fragment_to {
        transactions_by_address
            .entry(prev.keys[id].to_address().into())
            .or_default()
            .insert(0, fragment.id().into())
    }
}

#[derive(Default, Debug)]
pub struct Model {
    pub states: BTreeMap<BlockId, State>,
    pub tips: BTreeSet<(u32, BlockId)>,
    pub fragments: BTreeMap<FragmentId, Fragment>,
    pub blocks_by_chain_length: BTreeMap<u32, BTreeSet<BlockId>>,
    pub block_meta: BTreeMap<BlockId, BlockMeta>,
}

impl Model {
    pub const BLOCK0_PARENT_ID: StorableHash = StorableHash::new([0u8; 32]);
    pub const BLOCK0_ID: StorableHash = StorableHash::new([1u8; 32]);

    pub fn new() -> Model {
        let initial_state = initial_state(10);

        let fragments: BTreeMap<FragmentId, Fragment> = initial_state
            .fragments
            .iter()
            .map(|f| (f.id().into(), f.clone()))
            .collect();

        let mut blocks_by_chain_length: BTreeMap<u32, BTreeSet<BlockId>> = Default::default();

        blocks_by_chain_length
            .entry(0)
            .or_default()
            .insert(Self::BLOCK0_ID);

        let mut block_meta = BTreeMap::new();

        block_meta.insert(
            Self::BLOCK0_ID,
            BlockMeta {
                chain_length: ChainLength::new(0),
                date: BlockDate {
                    epoch: EpochNumber::new(0),
                    slot_id: SlotId::new(0),
                },
                parent_hash: Self::BLOCK0_PARENT_ID,
            },
        );

        Model {
            states: BTreeMap::from_iter(vec![(Self::BLOCK0_ID.clone(), initial_state)]),
            tips: BTreeSet::from_iter(vec![(0, Self::BLOCK0_ID)]),
            fragments,
            blocks_by_chain_length,
            block_meta,
        }
    }

    pub fn add_block(
        &mut self,
        parent_id: &BlockId,
        block_id: &BlockId,
        block_date: &BlockDate,
        chain_length: &ChainLength,
        spec: Vec<TransactionSpec>,
    ) {
        let parent_chain_length = u32::from(chain_length).checked_sub(1).unwrap();
        self.tips.remove(&(parent_chain_length, parent_id.clone()));

        let previous_state = self
            .states
            .get(&parent_id)
            .cloned()
            .expect("parent not found");

        let new_state = new_state(&previous_state, block_id.clone(), spec);

        for fragment in new_state.fragments.iter() {
            self.fragments
                .insert(fragment.id().into(), fragment.clone());
        }

        self.states.insert(block_id.clone(), new_state);

        self.blocks_by_chain_length
            .entry(chain_length.into())
            .or_default()
            .insert(block_id.clone());

        self.block_meta.insert(
            block_id.clone(),
            BlockMeta {
                chain_length: chain_length.clone(),
                date: block_date.clone(),
                parent_hash: parent_id.clone(),
            },
        );

        self.tips.insert((chain_length.into(), block_id.clone()));
    }

    pub fn get_branches(&self) -> Vec<BlockId> {
        self.tips.iter().map(|(_, v)| v.clone()).collect()
    }

    pub fn get_state_refs(&self) -> impl Iterator<Item = (&BlockId, &State)> {
        self.states.iter()
    }

    pub fn get_blocks_by_chain_length(
        &self,
        chain_length: &ChainLength,
    ) -> Option<&BTreeSet<BlockId>> {
        self.blocks_by_chain_length.get(&chain_length.into())
    }

    pub fn get_block_meta(&self, block_id: &BlockId) -> Option<&BlockMeta> {
        self.block_meta.get(block_id)
    }

    pub fn get_fragment(&self, fragment_id: &FragmentId) -> Option<&Fragment> {
        self.fragments.get(fragment_id)
    }
}
